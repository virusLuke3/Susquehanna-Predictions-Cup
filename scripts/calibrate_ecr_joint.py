#!/usr/bin/env python3
"""Offline chronological calibration and three-rule, 25,000-capital replay.

No credentials/network. Recorded depth permits a visible-depth fill scenario,
not reconstruction of queue position, historical ownership or actual latency.
"""

import argparse
import hashlib
import json
from bisect import bisect_left
from collections import Counter, defaultdict
from datetime import datetime, timezone
from decimal import Decimal as D
from pathlib import Path
from statistics import mean, stdev
from types import SimpleNamespace

from sigbot.domain import utc_time
from sigbot.live import code_hash
from sigbot.strategies.ecr_sig_v1 import core, joint, reference


def write(path, value):
    path.write_text(json.dumps(value, default=str, indent=2) + "\n")


def quantile(values, percent):
    values = sorted(values)
    return values[min(len(values) - 1, (len(values) - 1) * percent // 100)]


def candidate(row):
    inst = SimpleNamespace(**row["instrument"])
    inst.tick, inst.step, inst.fee_per_share = D(inst.tick), D(inst.step), D(inst.fee_per_share)
    return reference.candidate(inst, row["reference"], utc_time(row["at"]))


def seek_time(stream, end, target):
    low, high = 0, end
    while high - low > 4_000_000:
        middle = (low + high) // 2
        stream.seek(middle)
        stream.readline()
        start = stream.tell()
        raw = stream.readline()
        if not raw or start >= end:
            high = middle
        elif utc_time(json.loads(raw)["at"]) < target:
            low = start
        else:
            high = middle
    stream.seek(low)
    return low


def extract(output, hours, validation_hours, recorded_inputs=None):
    with Path("runtime/readonly/ecr-execution-20261021/forward-inputs-v2.jsonl").open() as stream:
        metadata = {r["sig_market_id"]: r for r in json.loads(stream.readline())["observations"]}
    source = Path(recorded_inputs or "runtime/readonly/strategies/ecr_sig_v1/book-observations.jsonl")
    end = source.stat().st_size
    with source.open("rb") as stream:
        stream.seek(max(0, end - 5000000))
        stream.readline()
        last = None
        while stream.tell() < end:
            raw = stream.readline()
            if stream.tell() <= end:
                last = json.loads(raw)
        cutoff = int(utc_time(last["at"]).timestamp())
    since = cutoff - hours * 3600
    counters, minutes = Counter(), {}
    digest = hashlib.sha256()
    with source.open("rb") as stream:
        start = seek_time(stream, end, datetime.fromtimestamp(since, timezone.utc))
        while stream.tell() < end:
            raw = stream.readline()
            if stream.tell() > end:
                break
            frame = json.loads(raw)
            at = int(utc_time(frame["at"]).timestamp())
            if at < since:
                continue
            digest.update(raw)
            counters["frames"] += 1
            for r in frame["observations"]:
                if recorded_inputs:
                    instrument = r["instrument"]
                    r = {"api_market_id": r["sig_market_id"], "sig": instrument.get("book"),
                         "external": [r["reference"]["book"]],
                         "strategy_ready": instrument.get("quality") == "HEALTHY"}
                mid = r.get("api_market_id")
                if mid not in metadata:
                    continue
                counters["observations"] += 1
                sig = r.get("sig")
                meta = metadata[mid]
                remotes = [x for x in r.get("external", []) if x.get("contract_id") == meta["reference"]["external_contract_id"]]
                if not sig or len(remotes) != 1:
                    counters["MISSING_PAIRED_BOOK"] += 1
                    continue
                inst = {**meta["instrument"], "book": sig, "book_at": sig["received_at"],
                        "quality": "HEALTHY" if r.get("strategy_ready") else "GAP"}
                row = {"at": frame["at"], "mid": mid, "instrument": inst,
                       "reference": {**meta["reference"], "book": remotes[0]}}
                try:
                    c = candidate(row)
                    c.sig_book.check(at)
                    c.external_books[0][1].check(at)
                except core.Reject as exc:
                    counters[str(exc)] += 1
                    continue
                # One actual observation per minute; never interpolate a missing minute.
                minutes[mid, at // 60] = row
    rows = sorted(minutes.values(), key=lambda r: (r["at"], r["mid"]))
    inputs = Path("runtime/readonly") / output.name / "RECORDED_INPUTS.jsonl"
    inputs.parent.mkdir(parents=True, exist_ok=True)
    with inputs.open("w") as stream:
        for row in rows:
            stream.write(json.dumps(row, default=str) + "\n")
    evidence = {"source": str(source), "end_byte": end, "seek_start_byte": start,
                "selected_sha256": digest.hexdigest(), "from": since, "to": cutoff,
                "train_end": cutoff - validation_hours * 3600,
                "population": sorted(metadata), "counts": dict(counters),
                "records": len(rows), "input_path": str(inputs.resolve())}
    write(output / "DATASET.json", evidence)
    return rows, evidence


def fit(rows, evidence, output):
    series = defaultdict(list)
    for row in rows:
        if int(utc_time(row["at"]).timestamp()) <= evidence["train_end"]:
            series[row["mid"]].append((int(utc_time(row["at"]).timestamp()), candidate(row)))
    cells, attempts, failures = defaultdict(list), Counter(), Counter()
    for _mid, local in series.items():
        times = [t for t, _ in local]
        for index, (at, c) in enumerate(local):
            for horizon in joint.HORIZONS:
                # Non-overlapping starts per instrument/horizon; cluster both parties
                # of the same election and time block together below.
                if index and at // horizon == local[index - 1][0] // horizon:
                    continue
                finish = bisect_left(times, at + horizon)
                for quantity in joint.QUANTITIES:
                    try:
                        ref = core.consume(c.external_books[0][1].bids, max(D(100), 5 * D(quantity)), False)[0]
                    except core.Reject:
                        failures["INSUFFICIENT_REFERENCE_QUANTITY"] += 1
                        continue
                    gap = ref - c.sig_book.ask
                    key = joint.group_key(c.office, gap, D(quantity)), str(horizon)
                    attempts[key] += 1
                    if finish == len(local) or times[finish] - (at + horizon) > 90:
                        failures["CENSORED_OR_FUTURE_BOOK_MISSING"] += 1
                        continue
                    path = local[index:finish + 1]
                    if any(b[0] - a[0] > 90 for a, b in zip(path, path[1:], strict=False)):
                        failures["PATH_GAP"] += 1
                        continue
                    try:
                        entry = core.consume(c.sig_book.asks, D(quantity), True)[0]
                        bids = [core.consume(x.sig_book.bids, D(quantity), False)[0] for _, x in path]
                    except core.Reject:
                        failures["INSUFFICIENT_RECORDED_QUANTITY"] += 1
                        continue
                    event = f"{c.state}:{c.office}"
                    cells[key].append((event, at // horizon, bids[-1] - bids[0],
                                       max(core.ZERO, entry - min(bids)), entry - bids[0]))
    groups = defaultdict(dict)
    for (key, horizon), values in cells.items():
        clustered = defaultdict(list)
        for event, block, move, _mae, _spread in values:
            clustered[event, block].append(move)
        changes = [mean(v) for v in clustered.values()]
        error = stdev(changes) / D(len(changes)).sqrt() if len(changes) > 1 else D(1)
        groups[key][horizon] = {
            "clusters": len(changes), "events": len({v[0] for v in values}),
            "samples": len(values), "attempts": attempts[key, horizon],
            "complete_fraction": str(D(len(values)) / attempts[key, horizon]),
            "mean_change": str(mean(changes)), "mean_standard_error": str(error),
            "mean_entry_spread": str(mean(v[4] for v in values)),
            "mae_quantiles": {str(q): str(quantile([v[3] for v in values], q)) for q in (80, 90, 95)},
        }
    model = {"target": "FUTURE_SIG_NET_BID_VWAP_CHANGE", "trained_through": evidence["train_end"],
             "valid_until": evidence["train_end"] + 3 * 86400, "groups": dict(groups),
             "dataset_hash": evidence["selected_sha256"], "training_missing": dict(failures),
             "feature_definition": "office, two-probability-point gap bucket, quantity bucket",
             "cluster_definition": "state/office and non-overlapping horizon block"}
    write(output / "MODEL.json", model)
    # A small declared training grid; validation is never used to choose a winner.
    grid = []
    for h in joint.HORIZONS:
        supported = [v[str(h)] for v in groups.values() if str(h) in v and v[str(h)]["clusters"] >= 20
                     and v[str(h)]["events"] >= 4 and D(v[str(h)]["complete_fraction"]) >= D('.8')]
        for q in (80, 90, 95):
            edges = [D(r["mean_change"]) - D(r["mean_entry_spread"]) - D('1.96') * D(r["mean_standard_error"])
                     - D('.0005') * h / 3600 for r in supported]
            grid.append({"horizon": h, "stop_quantile": q, "supported_cells": len(supported),
                         "mean_conservative_entry_edge": str(mean(edges)) if edges else None,
                         "mean_stop": str(mean(D(r["mae_quantiles"][str(q)]) for r in supported)) if supported else None})
    write(output / "TRAINING_GRID.json", grid)
    return model


def replay(rows, evidence, parameters, model, variant):
    cash, realized = D(100000), D(0)
    positions, latest, cooldown, rejected = {}, {}, {}, Counter()
    trades, curve = [], []
    high, max_drawdown = parameters.strategy_cash, D(0)
    for row in rows:
        at = int(utc_time(row["at"]).timestamp())
        if at <= evidence["train_end"]:
            continue
        c, mid = candidate(row), row["mid"]
        latest[mid] = c
        held = positions.get(mid)
        if held:
            try:
                net = core.consume(c.sig_book.bids, held["quantity"], False)[0]
                if variant == "legacy_baseline":
                    reason = core.exit_reason(held["plan"], net, c.external_books[0][1].bid, at,
                                              holding_started_at=held["opened"])
                elif variant == "calibrated_fixed":
                    reason = ("STOP_TRIGGER" if net <= held["entry"] - held["plan"].stop_trigger_per_share
                              else "TAKE_PROFIT" if net >= held["entry"] + held["plan"].target_gain_per_share
                              else "TIME_EXIT" if at >= held["opened"] + held["holding"] else None)
                else:
                    reason, _ = joint.exit_reason(c, held["quantity"], held["plan"], held["opened"], at,
                                                  parameters.model_copy(update={"holding_seconds": held["holding"]}), model)
                held["exit_task"] = held.get("exit_task") or reason
                if held["exit_task"]:
                    qty = core.floor_lot(min(held["quantity"], core.depth(c.sig_book.bids, c.sig_book.bid, core.ONE) * parameters.depth_fraction,
                                             parameters.child_cash / c.sig_book.bid), c.lot)
                    if qty <= 0:
                        raise core.Reject("EXIT_BLOCKED_ZERO_CAPACITY")
                    price = core.consume(c.sig_book.bids, qty, False)[0]
                    pnl = qty * (price - held["entry"])
                    cash += qty * price
                    realized += pnl
                    held["quantity"] -= qty
                    trades.append({"side": "SELL", "at": at, "market": mid, "quantity": str(qty),
                                   "price": str(price), "pnl": str(pnl), "reason": held["exit_task"]})
                    if not held["quantity"]:
                        del positions[mid]
                        cooldown[mid] = at
            except core.Reject as exc:
                rejected[str(exc)] += 1
        if mid not in positions and at - cooldown.get(mid, 0) >= 21600:
            cost = sum((v["entry"] * v["quantity"] for v in positions.values()), D(0))
            groups = [{}, {}, {}, {}]
            for v in positions.values():
                for g, key in zip(groups, (v["plan"].event_key, v["plan"].state, v["plan"].shock_group, v["plan"].direction_bucket), strict=True):
                    g[key] = g.get(key, D(0)) + v["entry"] * v["quantity"]
            account = core.Account(D(100000), cash, D(100000), D(100000), D(100000),
                                   total_exposure=cost, race_exposure=groups[0], state_exposure=groups[1],
                                   shock_exposure=groups[2], direction_exposure=groups[3],
                                   occupied_events={v["plan"].event_key for v in positions.values()})
            try:
                if realized <= -parameters.daily_loss:
                    raise core.Reject("DAILY_STRATEGY_LOSS")
                p = parameters.model_copy(update={"economic_revision": "legacy_v11"}) if variant == "legacy_baseline" else parameters
                plan = reference.proposal(c, account, utc_time(row["at"]), p)
                if cost + plan.total_reserved > parameters.strategy_cash or plan.total_reserved > cash - (D(100000) - parameters.strategy_cash):
                    raise core.Reject("STRATEGY_25000_CAPITAL")
                planned = sum((v["quantity"] * (v["plan"].stop_trigger_per_share + p.exit_buffer) for v in positions.values()), D(0))
                if variant != "legacy_baseline" and planned + plan.quantity * (plan.stop_trigger_per_share + p.exit_buffer) > p.total_planned_risk:
                    raise core.Reject("TOTAL_PLANNED_RISK")
                holding = 21600 if variant == "legacy_baseline" else joint.forecast(c, plan.quantity, model, p)["horizon"]
                price = core.consume(c.sig_book.asks, plan.quantity, True)[0]
                cash -= price * plan.quantity
                positions[mid] = {"quantity": plan.quantity, "entry": price, "plan": plan, "opened": at, "holding": holding}
                trades.append({"side": "BUY", "at": at, "market": mid, "quantity": str(plan.quantity), "price": str(price),
                               "stop": str(plan.stop_trigger_per_share), "holding": holding})
            except core.Reject as exc:
                rejected[str(exc)] += 1
        marked, unavailable = D(0), 0
        for key, v in positions.items():
            current = latest.get(key)
            if current and 0 <= at - current.sig_book.verified_at <= 5:
                try:
                    marked += core.consume(current.sig_book.bids, v["quantity"], False)[0] * v["quantity"]
                    continue
                except core.Reject:
                    pass
            unavailable += 1
        if not unavailable:
            nav = parameters.strategy_cash + cash - D(100000) + marked
            high = max(high, nav)
            max_drawdown = max(max_drawdown, high - nav)
            curve.append({"at": at, "nav": str(nav)})
    return {"variant": variant, "parameters": parameters.model_dump(mode="json"), "source_hash": code_hash(),
            "layer": "REAL_DATA_BACKTEST", "execution_cost_grade": "PARTIAL_RECORDED_VISIBLE_DEPTH_ZERO_LATENCY",
            "buys": sum(t["side"] == "BUY" for t in trades), "sells": sum(t["side"] == "SELL" for t in trades),
            "realized_pnl_scenario": str(realized), "max_drawdown_on_complete_marks": str(max_drawdown),
            "complete_nav_samples": len(curve), "last_complete_nav": curve[-1] if curve else None,
            "open_positions": [{"market": k, "quantity": str(v["quantity"]), "cost": str(v["quantity"] * v["entry"]),
                                "status": "RIGHT_CENSORED", "exit_task": v.get("exit_task")} for k, v in positions.items()],
            "rejections": dict(rejected), "trades": trades, "actual_orders": 0, "network_calls": 0,
            "limitations": ["Unobserved/insufficient future books remain missing, never zero-return labels",
                            "Historical own depth, queue position, actual request latency and other-strategy inventory unavailable",
                            "Passive fills are not simulated; exits use observed executable bids only",
                            "Missing portfolio marks are not zero and are excluded from complete NAV metrics"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    parser.add_argument("--hours", type=int, default=24)
    parser.add_argument("--validation-hours", type=int, default=8)
    parser.add_argument("--reuse-inputs", action="store_true")
    parser.add_argument("--parameters", help="Replay this exact frozen deployment parameter file")
    parser.add_argument("--recorded-inputs", help="All-222 paired depth log produced by the joint runtime")
    args = parser.parse_args()
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    if args.reuse_inputs:
        evidence = json.loads((output / "DATASET.json").read_text())
        with Path(evidence.get("input_path", output / "RECORDED_INPUTS.jsonl")).open() as stream:
            rows = [json.loads(r) for r in stream]
    else:
        rows, evidence = extract(output, args.hours, args.validation_hours, args.recorded_inputs)
    print(json.dumps({"stage": "inputs", **{k: v for k, v in evidence.items() if k != "population"}}), flush=True)
    if args.parameters:
        parameters = reference.Parameters.model_validate_json(Path(args.parameters).read_text())
        model = json.loads(Path(parameters.model_file).read_text())
    else:
        model = fit(rows, evidence, output)
        raw = (output / "MODEL.json").read_bytes()
        parameters = reference.Parameters(economic_revision="joint_v2", strategy_cash="25000",
                                          model_file=str(output / "MODEL.json"), model_hash=hashlib.sha256(raw).hexdigest())
    write(output / "PARAMETERS.json", parameters.model_dump(mode="json"))
    summary = {}
    for variant in ("legacy_baseline", "calibrated_fixed", "dynamic_joint"):
        result = replay(rows, evidence, parameters, model, variant)
        write(output / (variant.upper() + ".json"), result)
        summary[variant] = {k: v for k, v in result.items() if k not in {"trades", "rejections", "parameters", "limitations"}}
        print(json.dumps({"stage": "replay", **summary[variant]}, default=str), flush=True)
    write(output / "COMPARISON.json", summary)


if __name__ == "__main__":
    main()
