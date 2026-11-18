"""Offline SSR diagnostics from hash-bound official receipts; never trades or fills gaps."""

from __future__ import annotations

import csv
import hashlib
import json
from collections import defaultdict
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

from sigbot.errors import SchemaMismatch
from sigbot.history import candles, stamp
from sigbot.strategies.semantic_sig_v1.official_rules import extract_info
from sigbot.strategies.semantic_sig_v1.registry import groups
from sigbot.strategies.semantic_sig_v1.research import captured_catalog

D = Decimal
STEP = timedelta(minutes=5)


def hashed_json(path, expected=None):
    raw = Path(path).read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    if expected is not None and digest != expected:
        raise SchemaMismatch("SSR_RECEIPT_HASH_MISMATCH")
    return json.loads(raw, parse_float=D), {"path": str(path), "sha256": digest}


def csv_write(path, rows, fields=None):
    with Path(path).open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fields or list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


class ReceiptReplay:
    """Feed the existing history validator, with no network or credentials."""

    def __init__(self, pages, tid, exchange):
        self.pages = iter(pages)
        self.tid, self.exchange, self.calls = tid, exchange, 0

    def call(self, capability, *, path_params, query):
        if (
            capability != "price_history"
            or path_params != {"id": self.exchange}
            or query["tournamentId"] != self.tid
        ):
            raise SchemaMismatch("SSR_REPLAY_SCOPE_MISMATCH")
        self.calls += 1
        return next(self.pages)


def load_history(study, registry):
    """Use the recorded per-market recovery assignment, not partial or duplicate pages."""
    study = Path(study)
    combined, manifest = hashed_json(study / "COMBINED_REAL_READ.json")
    start, end = map(stamp, [combined["window_from_inclusive"], combined["window_to_exclusive"]])
    assignment = combined["source_assignment_by_market"]
    mapped = {m["sig_market_id"]: m for m in registry["markets"]}
    if set(assignment) != set(mapped) or combined["resolution"] != "5m":
        raise SchemaMismatch("SSR_HISTORY_CATALOG_SET_CHANGED")
    reports = [study / "REAL_READ.json"] + sorted(study.glob("RECOVERY_*.json"))
    pages, receipts, report_files = defaultdict(list), [], [manifest]
    failures, discarded = [], 0
    parent_run = None
    for path in reports:
        report, source = hashed_json(path)
        report_files.append(source)
        if path.name == "REAL_READ.json":
            parent_run = report["run_id"]
            scope_valid = report["tournament_id"] == registry["tournament_id"]
        else:
            # Recovery records bind to the original scope; Candle has no tournamentId field.
            scope_valid = report["parent_run_id"] == parent_run and all(
                stamp(report[key]) == boundary
                for key, boundary in [
                    ("window_from_inclusive", start),
                    ("window_to_exclusive", end),
                ]
            )
        if not scope_valid:
            raise SchemaMismatch("SSR_HISTORY_TOURNAMENT_CHANGED")
        failures.extend(report.get("errors", []))
        for receipt in report["request_receipts"]:
            if receipt["http_status"] != 200:
                raise SchemaMismatch("SSR_UNEXPECTED_RESPONSE_STATUS")
            body, evidence = hashed_json(
                receipt["private_evidence_path"], receipt["response_sha256"]
            )
            if not receipt["schema_validated"] or not receipt["scope_validated"]:
                raise SchemaMismatch("SSR_UNVALIDATED_ORIGINAL_RECEIPT")
            evidence.update(capability=receipt["capability"], received_at=receipt["received_at"])
            receipts.append(evidence)
            if receipt["capability"] != "price_history":
                continue
            mid = body["marketId"]
            if assignment.get(mid) != report["run_id"]:
                discarded += 1
                continue
            if mid not in mapped or body["exchangeId"] != mapped[mid]["exchange_id"]:
                raise SchemaMismatch("SSR_HISTORY_ID_CHANGED")
            pages[mid].append((body, evidence))
    result, bar_sources = {}, {}
    for mid, market in mapped.items():
        ordered = sorted(pages[mid], key=lambda page: stamp(page[0]["from"]))
        reader = ReceiptReplay(
            [b for b, _ in ordered], registry["tournament_id"], market["exchange_id"]
        )
        validated = candles(
            reader,
            mid,
            market["exchange_id"],
            registry["tournament_id"],
            start,
            end,
            resolution="5m",
        )
        if reader.calls != len(ordered):
            raise SchemaMismatch("SSR_UNUSED_OR_DUPLICATED_HISTORY_PAGE")
        result[mid] = {}
        for body, evidence in ordered:
            for bar in body["candles"]:
                at = stamp(bar["time"])
                # Official VWAP is a trade-volume mean; retain sparse or invalid VWAP as missing.
                value = bar["vwap"]
                if value is not None:
                    value = D(value)
                    if not value.is_finite() or not 0 <= value <= 1:
                        raise SchemaMismatch("SSR_INVALID_VWAP")
                    # IEEE rounding in official JSON can put VWAP just outside an exact OHLC edge.
                    if not D(bar["low"]) - D("1e-12") <= value <= D(bar["high"]) + D("1e-12"):
                        raise SchemaMismatch("SSR_VWAP_OUTSIDE_OHLC")
                result[mid][at] = value
                bar_sources[mid, at] = evidence["sha256"]
        if len(result[mid]) != len(validated["candles"]):
            raise SchemaMismatch("SSR_DUPLICATE_BAR")
    return (
        result,
        bar_sources,
        {
            "layer": "LOCAL_REAL_READ",
            "start": start.isoformat(),
            "end": end.isoformat(),
            "raw_nonempty_5m_candles": sum(map(len, result.values())),
            "catalog_count": len(mapped),
            "complete_paged_windows": len(result),
            "raw_receipts_hash_verified": len(receipts),
            "raw_receipts": receipts,
            "source_reports": report_files,
            "discarded_recovery_unassigned_pages": discarded,
            "original_failures_preserved": failures,
            "source_assignment": assignment,
            "forward_fill": False,
            "vwap_semantics": "OFFICIAL_TRADE_VOLUME_WEIGHTED_YES_MEAN",
            "same_bucket_is_strict_synchronous": False,
            "historical_role": "EXPLORATORY_ALREADY_USED_FOR_STRATEGY_SELECTION",
            "tournament_scope_evidence": "Original explicit tournament report and validated receipts; recovery bound to parent_run_id and exact window. Candle response itself has no tournamentId.",
        },
    )


RACE_FACTS = (
    "raceId",
    "stageId",
    "usState",
    "raceName",
    "raceStage",
    "officeLevel",
    "electionDate",
    "resolutionType",
)


def audit_pairs(markets, versions):
    """Title grouping produces candidates; official event metadata must agree."""
    rules = {r["sig_market_id"]: r for r in versions}
    if len(rules) != len(versions):
        raise SchemaMismatch("SSR_AMBIGUOUS_RULE_VERSION")
    for market in markets:
        rule = rules[market["sig_market_id"]]
        if hashlib.sha256(rule["text"].encode()).hexdigest() != rule["rules_hash"]:
            raise SchemaMismatch("SSR_RULE_TEXT_HASH_CHANGED")
        parsed = extract_info(market, *rule["source_receipts"])
        if (
            parsed["text"] != rule["text"]
            or parsed["contract_details"] != market["official_contract_details"]
        ):
            raise SchemaMismatch("SSR_RULE_RECONSTRUCTION_CHANGED")
    result, excluded = [], []
    for race, members in sorted(groups(markets).items()):
        if any(m["scope"] != "RACE" for m in members):
            excluded.extend(m["sig_market_id"] for m in members)
            continue
        parties = {m["party_code"]: m for m in members}
        if len(parties) != len(members) or not {"D", "R"} <= parties.keys():
            raise SchemaMismatch("SSR_D_R_PAIR_NOT_UNIQUE")
        d, r = parties["D"], parties["R"]
        rd, rr = rules[d["sig_market_id"]], rules[r["sig_market_id"]]
        left, right = rd["contract_details"], rr["contract_details"]
        metadata_matches = all(left.get(k) and left.get(k) == right.get(k) for k in RACE_FACTS)
        party_match = (
            rd["selected_party_label_raw"] == "Democratic Party"
            and rr["selected_party_label_raw"] == "Republican Party"
        )
        if not metadata_matches or not party_match:
            # Do not publish a candidate assembled from inconsistent specific races.
            raise SchemaMismatch("SSR_OFFICIAL_SAME_RACE_PAIR_MISMATCH")
        fusion = rd["fusion_ticket_multiple_yes"] or rr["fusion_ticket_multiple_yes"]
        result.append(
            {
                "pair_id": race,
                "office": d["office"],
                "state": d["state"],
                "district": d["district"],
                "D_market_id": d["sig_market_id"],
                "R_market_id": r["sig_market_id"],
                "D_exchange_id": d["exchange_id"],
                "R_exchange_id": r["exchange_id"],
                "official_race_id": left["raceId"],
                "official_stage_id": left["stageId"],
                "race_name": left["raceName"],
                "race_stage": left["raceStage"],
                "election_date": left["electionDate"],
                "official_event_metadata_match": True,
                "I_Other_market_ids": "|".join(
                    m["sig_market_id"] for m in members if m["party_code"] not in {"D", "R"}
                ),
                "I_Other_labels_raw": "|".join(
                    m["selected_party_label_raw"]
                    for m in members
                    if m["party_code"] not in {"D", "R"}
                ),
                "unlisted_other_winner_retained": True,
                "D_rules_hash": rd["rules_hash"],
                "R_rules_hash": rr["rules_hash"],
                "D_rule_source": rd["source_url"],
                "R_rule_source": rr["source_url"],
                "fusion_ticket_multiple_yes_allowed": fusion,
                "actual_D_R_candidate_nonoverlap": "UNKNOWN",
                "historical_effective_rules": "UNKNOWN",
                "rules_status": "REVIEW_REQUIRED",
                "mutual_exclusion_verified": False,
                "upstream_unconditional_mutex_status": "REJECTED_BY_RULES"
                if fusion
                else "UNVERIFIED",
                "normal_min_payout_verified": None,
                "trade_enabled": False,
                "blockers": "PER_PAIR_MUTEX_PROOF_MISSING|HUMAN_REVIEW_MISSING|HISTORICAL_RULE_EFFECTIVITY_UNKNOWN"
                + ("|OFFICIAL_FUSION_CLAUSE" if fusion else ""),
            }
        )
    return result, excluded


def verify_catalog(registry, path):
    from sigbot.strategies.prc_sig_v1.catalog import catalog_rows

    raw, tid, receipts, account_hash = captured_catalog(path)
    rows = {m["api_market_id"]: m for m in catalog_rows(raw, tid)}
    if tid != registry["tournament_id"] or set(rows) != {
        m["sig_market_id"] for m in registry["markets"]
    }:
        raise SchemaMismatch("SSR_CURRENT_CATALOG_SCOPE_MISMATCH")
    for market in registry["markets"]:
        item = rows[market["sig_market_id"]]
        if (
            item["exchange_ids"] != [market["exchange_id"]]
            or item["title"] != market["market_title"]
        ):
            raise SchemaMismatch("SSR_CATALOG_BINDING_MISMATCH")
    return tid, receipts, account_hash


def quantile(values, fraction):
    if not values:
        return None
    values = sorted(values)
    index = D(len(values) - 1) * D(str(fraction))
    low = int(index)
    return values[low] + (values[min(low + 1, len(values) - 1)] - values[low]) * (index - low)


def coverage(grid, d, r):
    ds, rs = {t for t in grid if d.get(t) is not None}, {t for t in grid if r.get(t) is not None}
    joint, union = ds & rs, ds | rs
    longest = current = 0
    for t in grid:
        current = current + 1 if t in joint else 0
        longest = max(longest, current)
    return {
        "expected_5m_buckets": len(grid),
        "D_observed": len(ds),
        "R_observed": len(rs),
        "joint_observed": len(joint),
        "joint_coverage": str(D(len(joint)) / len(grid)),
        "union_observed": len(union),
        "joint_over_union": str(D(len(joint)) / len(union)) if union else None,
        "D_only": len(ds - rs),
        "R_only": len(rs - ds),
        "neither_observed": len(set(grid) - union),
        "max_contiguous_joint_minutes": longest * 5,
    }


def episodes(grid, residuals, threshold, cooldown=3):
    """Missing buckets cannot end an episode, grant reentry, or establish first return."""
    if (
        not threshold.is_finite()
        or threshold <= 0
        or isinstance(cooldown, bool)
        or not isinstance(cooldown, int)
        or cooldown < 1
        or any(b - a != STEP for a, b in zip(grid, grid[1:], strict=False))
    ):
        raise SchemaMismatch("SSR_EPISODE_CONFIGURATION_INVALID")
    result, active, quiet = [], None, 0
    previous, last_positive = None, None
    for index, at in enumerate(grid):
        value = residuals.get(at)
        if value is None:
            if active is not None:
                active["missing_buckets"] += 1
                if active["first_observed_recovery_at"] is None:
                    if active["first_gap_before_return_at"] is None:
                        active["first_gap_before_return_at"] = at.isoformat()
                    active["gaps_before_first_return"] += 1
                quiet = 0
            previous = None
            continue
        if active is None and value >= threshold:
            active = {
                "start": at.isoformat(),
                "entry_residual": str(value),
                "peak_residual": str(value),
                "left_censored": index == 0 or previous is None,
                "missing_buckets": 0,
                "gaps_before_first_return": 0,
                "observed_buckets": 0,
                "threshold_exceedance_buckets": 0,
                "first_observed_recovery_at": None,
                "first_gap_before_return_at": None,
                "recovery_interval_lower_utc": None,
                "recovery_elapsed_minutes": None,
                "cooldown_complete_at": None,
                "right_censored": True,
                "economic_exit_observed": False,
                "capital_released_verified": False,
                "eligible_trade_reentry": False,
            }
            quiet, last_positive = 0, at
        if active is not None:
            active["observed_buckets"] += 1
            active["threshold_exceedance_buckets"] += int(value >= threshold)
            active["peak_residual"] = str(max(D(active["peak_residual"]), value))
            active["last_observed_at"] = at.isoformat()
            if value > 0:
                quiet, last_positive = 0, at
            else:
                if active["first_observed_recovery_at"] is None:
                    # Times are bucket-END observation markers, never trade/fill timestamps.
                    active["first_observed_recovery_at"] = (at + STEP).isoformat()
                    # A hidden return followed by a recross may occur in ANY earlier gap.
                    # The last positive quote is not a lower bound on first passage.
                    active["recovery_interval_lower_utc"] = (
                        active["first_gap_before_return_at"] or (last_positive + STEP).isoformat()
                    )
                    active["recovery_elapsed_minutes"] = int(
                        (at - stamp(active["start"])).total_seconds() / 60
                    )
                quiet += 1
                if quiet >= cooldown:
                    active["cooldown_complete_at"] = (at + STEP).isoformat()
                    active["right_censored"] = False
                    result.append(active)
                    active, quiet = None, 0
        previous = value
    if active is not None:
        result.append(active)
    for i, episode in enumerate(result, 1):
        episode["episode_number"] = i
        episode["entry_observable_bucket_end"] = (stamp(episode["start"]) + STEP).isoformat()
        episode["first_return_continuously_observed"] = (
            episode["first_observed_recovery_at"] is not None
            and not episode["left_censored"]
            and episode["gaps_before_first_return"] == 0
        )
        episode["fully_observed_episode"] = (
            episode["first_return_continuously_observed"]
            and not episode["right_censored"]
            and episode["missing_buckets"] == 0
        )
        episode["interval_censored_first_return"] = (
            episode["first_observed_recovery_at"] is not None
            and episode["gaps_before_first_return"] > 0
        )
        episode["strict_synchronous"] = False
    return result


def diagnostics(pairs, bars, bar_sources, start, end, config):
    grid = [start + i * STEP for i in range(int((end - start) / STEP))]
    coverage_rows, diagnostic_rows, bar_rows, all_episodes, sensitivity = [], [], [], [], []
    for pair in pairs:
        key, dm, rm = pair["pair_id"], pair["D_market_id"], pair["R_market_id"]
        d, r = bars[dm], bars[rm]
        residuals = {
            t: d[t] + r[t] - 1 for t in grid if d.get(t) is not None and r.get(t) is not None
        }
        for t, value in residuals.items():
            bar_rows.append(
                {
                    "pair_id": key,
                    "bucket_start_utc": t.isoformat(),
                    "bucket_end_utc": (t + STEP).isoformat(),
                    "mean_yes_D": str(d[t]),
                    "mean_yes_R": str(r[t]),
                    "residual_proxy": str(value),
                    "derived_double_NO_mean_sum": str(1 - value),
                    "D_receipt_sha256": bar_sources[dm, t],
                    "R_receipt_sha256": bar_sources[rm, t],
                    "strict_synchronous": False,
                    "executable_NO_prices": False,
                }
            )
        periods = [("ALL_7D", grid)] + [
            (
                (start + timedelta(days=i))
                .astimezone(ZoneInfo("Asia/Shanghai"))
                .date()
                .isoformat(),
                grid[i * 288 : (i + 1) * 288],
            )
            for i in range(7)
        ]
        for label, times in periods:
            coverage_rows.append({"pair_id": key, "period": label, **coverage(times, d, r)})
            values = [residuals[t] for t in times if t in residuals]
            diagnostic_rows.append(
                {
                    "pair_id": key,
                    "period": label,
                    "joint_observations": len(values),
                    "residual_min": min(values) if values else None,
                    "residual_p10": quantile(values, ".1"),
                    "residual_median": quantile(values, ".5"),
                    "residual_p90": quantile(values, ".9"),
                    "residual_max": max(values) if values else None,
                    "positive_residual_observations": sum(v > 0 for v in values),
                    "entry_threshold_observations": sum(
                        v >= D(config["entry_residual"]) for v in values
                    ),
                    "residual_model": "VWAP_D_PLUS_VWAP_R_MINUS_ONE_PROXY_NOT_ARB",
                }
            )
        for threshold in [config["entry_residual"], *config["sensitivity_entry_residuals"]]:
            events = episodes(grid, residuals, D(threshold), config["cooldown_contiguous_buckets"])
            all_episodes.extend({"pair_id": key, "threshold": threshold, **e} for e in events)
            sensitivity.append(
                {
                    "pair_id": key,
                    "threshold": threshold,
                    "episodes": len(events),
                    "threshold_observations": sum(v >= D(threshold) for v in residuals.values()),
                    "observed_returns": sum(
                        e["first_observed_recovery_at"] is not None for e in events
                    ),
                    "continuous_first_returns": sum(
                        e["first_return_continuously_observed"] for e in events
                    ),
                    "fully_observed_episodes": sum(e["fully_observed_episode"] for e in events),
                    "left_censored": sum(e["left_censored"] for e in events),
                    "right_censored": sum(e["right_censored"] for e in events),
                    "interval_censored_first_returns": sum(
                        e["interval_censored_first_return"] for e in events
                    ),
                }
            )
    return coverage_rows, diagnostic_rows, bar_rows, all_episodes, sensitivity
