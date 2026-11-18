"""Small S3 ridge models, chronological controls and quote-side execution diagnostics.

Statistics use floats. Cash and per-share returns use Decimal. No correlation,
direction, coefficient, fee or execution capacity is supplied by the old graph.
"""

from __future__ import annotations

import hashlib
import json
import math
import random
import statistics
from collections import Counter, defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from sigbot.domain import content_hash
from sigbot.strategies.semantic_sig_v1.registry import write_json
from sigbot.strategies.semantic_sig_v1.timeseries import frames

FEATURES = ["target_change", "target_lag", "national", "state", "office", "spread", "platform"]
PROTOCOL = {
    "strategy": "S3_SINGLE_LAGGING_YES_CONTRACT",
    "horizon_seconds": 300,
    "cadence_seconds": 60,
    "split_fractions": [0.6, 0.2, 0.2],
    "purge_seconds": 300,
    "embargo_seconds": 300,
    "ridge_penalty": 1.0,
    "minimum_train_rows": 120,
    "minimum_validation_rows": 40,
    "minimum_test_rows": 40,
    "minimum_test_shock_blocks": 30,
    "minimum_test_dates": 2,
    "minimum_national_events": 8,
    "minimum_state_events": 2,
    "fdr_alpha": 0.05,
    "bootstrap_draws": 399,
    "control_graphs": 5,
    "latency_stress_seconds": [60, 120],
    "epsilon": 0.005,
    "features": FEATURES,
    "platform_factor_scope": "SIMULTANEOUS_PRICE_CHANGE_PROXY_NOT_IDENTIFIED_PLATFORM_PROGRAM",
    "selection_policy": "FIXED_TARGETS_ALL_SAME_STATE_SAME_PARTY_NEIGHBORS_NO_TEST_TUNING",
    "outcome_assumption": "NONE_CROSS_PARTY_COMPLEMENTS_NOT_USED",
    "execution": "BUY_EXTERNAL_ASK_EXIT_EXTERNAL_BID_ONLY_TARGET_NO_PAIR_LEG",
    "exit": "HORIZON_OR_MODEL_DATA_RULE_INVALIDATION_NO_PROFIT_FLOOR",
}


def logit(quote, epsilon):
    mid = (float(quote["bid"]) + float(quote["ask"])) / 2
    value = min(1 - epsilon, max(epsilon, mid))
    return math.log(value / (1 - value))


def logistic(value):
    return 1 / (1 + math.exp(-max(-40, min(40, value))))


def ridge(rows, y, penalty=1.0):
    if not rows or len(rows) != len(y):
        raise ValueError("SEMANTIC_MODEL_EMPTY_TRAINING")
    width = len(rows[0])
    means = [statistics.fmean(r[j] for r in rows) for j in range(width)]
    scales = [statistics.pstdev(r[j] for r in rows) or 1.0 for j in range(width)]
    x = [[1.0, *((r[j] - means[j]) / scales[j] for j in range(width))] for r in rows]
    matrix = [
        [sum(r[i] * r[j] for r in x) + (penalty if i == j and i else 0) for j in range(width + 1)]
        + [sum(r[i] * target for r, target in zip(x, y, strict=True))]
        for i in range(width + 1)
    ]
    for i in range(width + 1):
        pivot = max(range(i, width + 1), key=lambda k: abs(matrix[k][i]))
        matrix[i], matrix[pivot] = matrix[pivot], matrix[i]
        denominator = matrix[i][i]
        if abs(denominator) < 1e-12:
            raise ValueError("SEMANTIC_MODEL_SINGULAR")
        matrix[i] = [v / denominator for v in matrix[i]]
        for k in range(width + 1):
            if k != i:
                scale = matrix[k][i]
                matrix[k] = [a - scale * b for a, b in zip(matrix[k], matrix[i], strict=True)]
    return {"means": means, "scales": scales, "coefficients": [r[-1] for r in matrix]}


def predict(model, row):
    if len(row) != len(model["means"]) or not all(math.isfinite(v) for v in row):
        raise ValueError("SEMANTIC_MODEL_FEATURES_INVALID")
    return model["coefficients"][0] + sum(
        c * (x - m) / scale
        for c, x, m, scale in zip(
            model["coefficients"][1:], row, model["means"], model["scales"], strict=True
        )
    )


def edges(instruments, targets):
    return [
        {"source": source, "target": target, "edge_id": f"S3::{source}::{target}"}
        for target in sorted(targets)
        if target in instruments
        for source, item in sorted(instruments.items())
        if item["state"] == instruments[target]["state"]
        and item["party"] == instruments[target]["party"]
        and item["event_id"] != instruments[target]["event_id"]
    ]


def representatives(instruments, party, exclude):
    # Exactly one same-party YES series per underlying official election event.
    # D/R/I and mechanical NO copies never multiply the factor denominator.
    result: dict[str, str] = {}
    for eid, item in sorted(instruments.items()):
        if item["party"] == party and item["event_id"] not in exclude:
            result.setdefault(item["event_id"], eid)
    return list(result.values())


def state_factor_coverage(instruments, edge, minimum):
    """Distinguish absent events from missing observations of existing events."""
    source, target = instruments[edge["source"]], instruments[edge["target"]]
    eligible = representatives(
        instruments, target["party"], {source["event_id"], target["event_id"]}
    )
    events = sorted(
        {instruments[e]["event_id"] for e in eligible if instruments[e]["state"] == target["state"]}
    )
    structural = len(events) < minimum
    return {
        "other_state_events": events,
        "available_other_events": len(events),
        "required_other_events": minimum,
        "status": "STRUCTURALLY_UNAVAILABLE" if structural else "OBSERVATION_COVERAGE_NOT_VERIFIED",
        "more_time_samples_can_resolve": False if structural else None,
        "scope": "CURRENT_FULL_STATE_FACTOR_PROTOCOL_ONLY_NOT_RULES_SCANNER",
    }


def feature_rows(series, instruments, edge, protocol):
    a, b = edge["source"], edge["target"]
    source, target = instruments[a], instruments[b]
    reps = representatives(instruments, target["party"], {source["event_id"], target["event_id"]})

    def office_kind(item):
        name = item["contract_details"].get("raceName", "").casefold()
        return next((kind for kind in ("senate", "house", "governor") if kind in name), None)

    horizon = protocol["horizon_seconds"] // protocol["cadence_seconds"]
    rows: list[dict] = []
    missing: Counter[str] = Counter()
    for i in range(2, len(series) - horizon):
        current, previous, older, future = (
            series[i],
            series[i - 1],
            series[i - 2],
            series[i + horizon],
        )
        if (
            current["at"] - previous["at"] != protocol["cadence_seconds"]
            or previous["at"] - older["at"] != protocol["cadence_seconds"]
            or future["at"] - current["at"] != protocol["horizon_seconds"]
        ):
            missing["noncontiguous_grid"] += 1
            continue
        # Intermediate target/source gaps forbid execution diagnostics across the gap.
        window = series[i - 2 : i + horizon + 1]
        if any(a not in s["quotes"] or b not in s["quotes"] for s in window):
            missing["source_or_target_gap"] += 1
            continue
        epsilon = protocol["epsilon"]

        def delta(eid, first=current, last=previous, epsilon=epsilon):
            return logit(first["quotes"][eid], epsilon) - logit(last["quotes"][eid], epsilon)

        national, state, office = [], [], []
        for eid in reps:
            if eid not in current["quotes"] or eid not in previous["quotes"]:
                continue
            value = delta(eid)
            national.append(value)
            if instruments[eid]["state"] == target["state"]:
                state.append(value)
            if office_kind(target) is not None and office_kind(instruments[eid]) == office_kind(
                target
            ):
                office.append(value)
        if len(national) < protocol["minimum_national_events"]:
            missing["insufficient_national_factor_events"] += 1
            continue
        quote, later = current["quotes"][b], future["quotes"][b]
        changes = sum(abs(x) > 1e-12 for x in national) / len(national)

        def mean(values):
            return statistics.fmean(values) if values else 0.0

        x = [
            delta(b),
            delta(b, previous, older),
            mean(national),
            mean(state),
            mean(office),
            float(quote["ask"]) - float(quote["bid"]),
            changes,
        ]
        label = logit(later, epsilon) - logit(quote, epsilon)
        rows.append(
            {
                "at": current["at"],
                "label_at": future["at"],
                "x": x,
                "source_delta": delta(a),
                "y": label,
                "mid_logit": logit(quote, epsilon),
                "quote": quote,
                "exit_quote": later,
                "latency_quotes": {
                    str(delay): series[i + delay // protocol["cadence_seconds"]]["quotes"][b]
                    for delay in protocol["latency_stress_seconds"]
                },
                "state_factor_complete": len(state) >= protocol["minimum_state_events"],
                "innovation": abs(delta(a)) > 1e-12 or abs(delta(b)) > 1e-12,
                "factor_events": len(national),
            }
        )
    return rows, dict(missing)


def split(rows, protocol):
    if not rows:
        return [], [], [], {}
    start, end = rows[0]["at"], rows[-1]["at"]
    validation_start = start + (end - start) * protocol["split_fractions"][0]
    test_start = start + (end - start) * sum(protocol["split_fractions"][:2])
    embargo, purge = protocol["embargo_seconds"], protocol["purge_seconds"]
    train = [r for r in rows if r["label_at"] < validation_start - purge]
    validation = [
        r
        for r in rows
        if r["at"] >= validation_start + embargo and r["label_at"] < test_start - purge
    ]
    test = [r for r in rows if r["at"] >= test_start + embargo]
    return (
        train,
        validation,
        test,
        {
            "validation_start": validation_start,
            "test_start": test_start,
            "purge_seconds": purge,
            "embargo_seconds": embargo,
            "train_rows": len(train),
            "validation_rows": len(validation),
            "test_rows": len(test),
            "train_last_label_at": train[-1]["label_at"] if train else None,
            "test_dates": sorted(
                {datetime.fromtimestamp(r["at"], timezone.utc).date().isoformat() for r in test}
            ),
        },
    )


def block_evidence(rows, differences, protocol, seed):
    blocks = defaultdict(list)
    for row, difference in zip(rows, differences, strict=True):
        if row["innovation"]:
            blocks[int(row["at"] // protocol["horizon_seconds"])].append(difference)
    values = [statistics.fmean(items) for items in blocks.values()]
    if not values:
        return {"effective_shock_blocks": 0, "improvement_ci_lower": None, "p_value": None}
    rng = random.Random(seed)
    dates = defaultdict(list)
    for block, items in blocks.items():
        date = (
            datetime.fromtimestamp(block * protocol["horizon_seconds"], timezone.utc)
            .date()
            .isoformat()
        )
        dates[date].append(statistics.fmean(items))

    def date_draw():
        sampled = [
            v
            for date in rng.choices(sorted(dates), k=len(dates))
            for v in rng.choices(dates[date], k=len(dates[date]))
        ]
        return statistics.fmean(sampled)

    bootstraps = sorted(date_draw() for _ in range(protocol["bootstrap_draws"]))
    observed = statistics.fmean(values)
    exceed = sum(
        statistics.fmean(
            v * sign
            for date in sorted(dates)
            for sign in [rng.choice([-1, 1])]
            for v in dates[date]
        )
        >= observed
        for _ in range(protocol["bootstrap_draws"])
    )
    return {
        "effective_shock_blocks": len(values),
        "improvement_ci_lower": bootstraps[int(len(bootstraps) * 0.025)],
        "p_value": (exceed + 1) / (protocol["bootstrap_draws"] + 1),
        "resampling_unit": "UTC_DATE_THEN_HORIZON_INNOVATION_BLOCK_NOT_RAW_QUOTES",
        "effective_dates": len(dates),
    }


def fdr(results):
    measured = sorted(
        ((r["p_value"], i) for i, r in enumerate(results) if r.get("p_value") is not None)
    )
    previous = 1.0
    for rank in range(len(measured), 0, -1):
        p, index = measured[rank - 1]
        previous = min(previous, p * len(measured) / rank)
        results[index]["fdr_q"] = previous


def fit_edge(series, instruments, edge, protocol):
    rows, missing = feature_rows(series, instruments, edge, protocol)
    train, validation, test, windows = split(rows, protocol)
    coverage = state_factor_coverage(instruments, edge, protocol["minimum_state_events"])
    structural = coverage["status"] == "STRUCTURALLY_UNAVAILABLE"
    result = {
        **edge,
        "windows": windows,
        "missing": missing,
        "state_factor_coverage": coverage,
        "fdr_q": None,
        "validated": False,
    }
    if any(
        len(part) < protocol[key]
        for part, key in [
            (train, "minimum_train_rows"),
            (validation, "minimum_validation_rows"),
            (test, "minimum_test_rows"),
        ]
    ):
        return {
            **result,
            "decision": "NEED_MORE_DATA",
            "reasons": ["INSUFFICIENT_CONTIGUOUS_REAL_QUOTE_WINDOWS"]
            + (["STATE_FACTOR_STRUCTURALLY_UNAVAILABLE"] if structural else []),
        }
    x, y = [r["x"] for r in train], [r["y"] for r in train]
    baseline = ridge(x, y, protocol["ridge_penalty"])
    innovation = ridge(x, [r["source_delta"] for r in train], protocol["ridge_penalty"])

    def augmented(row):
        return [*row["x"], row["source_delta"] - predict(innovation, row["x"])]

    semantic = ridge([augmented(r) for r in train], y, protocol["ridge_penalty"])
    predictions = [predict(semantic, augmented(r)) for r in test]
    baseline_errors = [(r["y"] - predict(baseline, r["x"])) ** 2 for r in test]
    semantic_errors = [(r["y"] - p) ** 2 for r, p in zip(test, predictions, strict=True)]
    evidence = block_evidence(
        test,
        [a - b for a, b in zip(baseline_errors, semantic_errors, strict=True)],
        protocol,
        edge["edge_id"],
    )
    losses = {
        "no_trade_mse": statistics.fmean(r["y"] ** 2 for r in test),
        "target_history_mse": None,
        "factor_baseline_mse": statistics.fmean(baseline_errors),
        "semantic_mse": statistics.fmean(semantic_errors),
        "validation_increment": statistics.fmean(
            (r["y"] - predict(baseline, r["x"])) ** 2
            - (r["y"] - predict(semantic, augmented(r))) ** 2
            for r in validation
        ),
    }
    own_history = ridge([r["x"][:2] for r in train], y, protocol["ridge_penalty"])
    losses["target_history_mse"] = statistics.fmean(
        (r["y"] - predict(own_history, r["x"][:2])) ** 2 for r in test
    )
    residual_buffer = math.sqrt(
        statistics.fmean((r["y"] - predict(semantic, augmented(r))) ** 2 for r in validation)
    )
    returns = defaultdict(list)
    next_entry = 0
    for row, prediction in zip(test, predictions, strict=True):
        estimated_bid = (
            logistic(row["mid_logit"] + prediction - residual_buffer) - float(row["x"][5]) / 2
        )
        if row["at"] < next_entry or estimated_bid <= float(row["quote"]["ask"]):
            continue
        next_entry = row["label_at"]  # No overlapping repeated use of the same target holding.
        for delay, quote in row["latency_quotes"].items():
            returns[delay].append(Decimal(row["exit_quote"]["bid"]) - Decimal(quote["ask"]))
    economic = {
        "kind": "QUOTE_SIDE_UPPER_BOUND_NOT_FILLED_PNL",
        "real_pnl": None,
        "capacity": None,
        "own_order_exclusion": "NOT_VERIFIED",
        "cost_adjusted_return": None,
        "fees": "NOT_VERIFIED_FOR_THIS_STRATEGY",
        "latency_diagnostics": {
            delay: {
                "nonoverlapping_signals": len(items),
                "gross_unit_quote_return": str(sum(items, Decimal(0))),
                "hypothetical_fee_stress": {
                    fee: str(sum(items, Decimal(0)) - 2 * Decimal(fee) * len(items))
                    for fee in ["0.005", "0.01", "0.02"]
                },
            }
            for delay, items in returns.items()
        },
        "fee_stress_is_assumption_not_platform_fee": True,
    }
    return {
        **result,
        **evidence,
        "scores": losses,
        "models": {"baseline": baseline, "innovation": innovation, "semantic": semantic},
        "residual_buffer": residual_buffer,
        "all_state_factors_complete": all(r["state_factor_complete"] for r in rows),
        "economic": economic,
        "decision": "NEED_MORE_DATA",
        "reasons": [],
    }


def validate(manifest_path, output, targets, protocol=None):
    protocol = dict(protocol or PROTOCOL)
    raw = Path(manifest_path).read_bytes()
    manifest = json.loads(raw)
    if (
        manifest.get("synthetic_data_used") is not False
        or manifest.get("data_provenance") != "EXISTING_OFFICIAL_REST_QUOTE_OBSERVATION_ARCHIVE"
    ):
        raise ValueError("SEMANTIC_REAL_DATASET_REQUIRED")
    if manifest["cadence_seconds"] != protocol["cadence_seconds"]:
        raise ValueError("SEMANTIC_PROTOCOL_GRID_MISMATCH")
    series, instruments = list(frames(manifest)), manifest["instruments"]
    candidate_edges = edges(instruments, targets)
    results = [fit_edge(series, instruments, e, protocol) for e in candidate_edges]
    # Rewire each target's entire incoming degree, rather than selecting the best
    # random edge after seeing results. Same-party, different-event controls only.
    controls: list[dict] = []
    for graph in range(protocol["control_graphs"]):
        control_edges: list[dict] = []
        for target in sorted(targets):
            incoming = [e for e in candidate_edges if e["target"] == target]
            if not incoming:
                continue
            choices = [
                eid
                for eid, m in sorted(instruments.items())
                if m["party"] == instruments[target]["party"]
                and m["state"] != instruments[target]["state"]
                and m["event_id"] != instruments[target]["event_id"]
            ]
            random.Random(f"SEMANTIC-CONTROL-{graph}-{target}").shuffle(choices)
            if len(choices) < len(incoming):
                raise ValueError("SEMANTIC_DEGREE_MATCHED_CONTROL_NOT_AVAILABLE")
            control_edges.extend(
                {
                    "source": source,
                    "target": target,
                    "edge_id": f"CONTROL::{graph}::{source}::{target}",
                }
                for source in choices[: len(incoming)]
            )
        controls.extend(fit_edge(series, instruments, e, protocol) for e in control_edges)
    fdr(results)
    for r in results:
        if not r.get("models"):
            continue
        reasons = []
        if (
            r["effective_shock_blocks"] < protocol["minimum_test_shock_blocks"]
            or len(r["windows"]["test_dates"]) < protocol["minimum_test_dates"]
        ):
            reasons.append("INSUFFICIENT_SHOCK_BLOCKS_OR_TEST_DATES")
        if not r["all_state_factors_complete"]:
            reasons.append("STATE_FACTOR_CONTROL_INCOMPLETE")
        if r["state_factor_coverage"]["status"] == "STRUCTURALLY_UNAVAILABLE":
            reasons.append("STATE_FACTOR_STRUCTURALLY_UNAVAILABLE")
        if not (
            r["improvement_ci_lower"] is not None
            and r["improvement_ci_lower"] > 0
            and r["fdr_q"] is not None
            and r["fdr_q"] <= protocol["fdr_alpha"]
            and r["scores"]["validation_increment"] > 0
        ):
            reasons.append("SEMANTIC_OOS_INCREMENT_NOT_PROVEN")
        eligible_controls = [c for c in controls if c["target"] == r["target"] and c.get("scores")]
        if not eligible_controls:
            reasons.append("RANDOM_GRAPH_CONTROL_UNAVAILABLE")
        elif r["scores"]["factor_baseline_mse"] - r["scores"]["semantic_mse"] <= max(
            c["scores"]["factor_baseline_mse"] - c["scores"]["semantic_mse"]
            for c in eligible_controls
        ):
            reasons.append("SEMANTIC_GRAPH_DOES_NOT_BEAT_RANDOM_CONTROL")
        reasons.extend(
            [
                "HISTORICAL_L2_CAPACITY_NOT_VERIFIED",
                "HISTORICAL_OWN_ORDER_EXCLUSION_NOT_VERIFIED",
                "STRATEGY_FEE_CONTRACT_NOT_VERIFIED",
                "HISTORICAL_RULES_PIT_NOT_VERIFIED",
            ]
        )
        r["reasons"] = reasons
        # Quote-only forecasts do not authorize executable-cost trading.
        r["decision"] = "NEED_MORE_DATA"
    result = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "protocol": protocol,
        "protocol_hash": content_hash(protocol),
        "dataset_manifest_sha256": hashlib.sha256(raw).hexdigest(),
        "dataset_manifest_path": str(manifest_path),
        "registry_sha256": manifest["registry_sha256"],
        "targets": sorted(targets),
        "candidate_edges": len(candidate_edges),
        "official_event_deduplication": True,
        "cross_party_complement_assumed": False,
        "control_graphs": protocol["control_graphs"],
        "relations": results,
        "random_controls": controls,
        "decision": "NEED_MORE_DATA",
        "live_eligible": False,
        "real_orders": 0,
        "real_pnl": None,
        "synthetic_data_used": False,
        "execution_replay": "QUOTE_SIDE_DIAGNOSTIC_EXECUTABLE_L2_REPLAY_BLOCKED",
    }
    write_json(Path(output) / "model_validation.json", result)
    write_json(
        Path(output) / "execution_replay.json",
        {
            "relations": [
                {"edge_id": r["edge_id"], "economic": r.get("economic"), "reasons": r["reasons"]}
                for r in results
            ],
            "real_pnl": None,
            "layer": "REAL_QUOTE_DIAGNOSTIC_NOT_EXECUTABLE_REPLAY",
        },
    )
    return result
