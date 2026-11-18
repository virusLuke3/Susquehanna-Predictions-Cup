"""Offline S3 trade-VWAP research. No quoted spreads, fills, fees or live admission."""

from __future__ import annotations

import csv
import hashlib
import itertools
import json
import random
import statistics
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

from sigbot.domain import content_hash
from sigbot.errors import SchemaMismatch
from sigbot.history import stamp
from sigbot.history_ssr import load_history
from sigbot.strategies.semantic_sig_v1.model import (
    edges,
    fdr,
    predict,
    ridge,
    state_factor_coverage,
)
from sigbot.strategies.semantic_sig_v1.registry import write_json
from sigbot.strategies.semantic_sig_v1.timeseries import metadata

STEP = 300
PROTOCOL: dict = {
    "version": "S3_TRADE_VWAP_2_DESCRIPTIVE_COVERAGE_ADDED",
    "price": "OFFICIAL_YES_NORMALIZED_TRADE_VWAP_NOT_BID_ASK",
    "target": "NEXT_COMPLETED_5M_BUCKET_VWAP_CHANGE",
    "sampling_seconds": STEP,
    "horizon_seconds": STEP,
    "split_fractions": [0.6, 0.2, 0.2],
    "purge_seconds": STEP,
    "embargo_seconds": STEP,
    "minimum_train_rows": 120,
    "minimum_validation_rows": 40,
    "minimum_test_rows": 40,
    "minimum_test_change_blocks": 30,
    "minimum_test_change_dates": 2,
    "minimum_national_events": 8,
    "minimum_complete_state_events": 2,
    "ridge_penalty": 1.0,
    "fdr_alpha": 0.05,
    "bootstrap_draws": 399,
    "random_controls": 5,
    "features": ["target_change", "target_lag", "national", "state", "office"],
    "source_feature": "SOURCE_CHANGE_RESIDUAL_AFTER_TRAIN_ONLY_BASELINE_REGRESSION",
    "missing": "NO_FORWARD_FILL_OR_INTERPOLATION",
    "availability": "ASSUMED_BUCKET_END_NOT_HISTORICAL_RECEIVE_TIME",
    "sample_role": "RETROSPECTIVE_TIME_HOLDOUT_ALREADY_EXPLORED_DATA",
    "causal_effect_identified": False,
    "amendment": "V1 had zero eligible models; add descriptive correlations without lowering model gates or changing horizons",
}


def office(item):
    name = item["contract_details"]["raceName"].casefold()
    return next((v for v in ("senate", "house", "governor") if v in name), None)


class TradeSeries:
    """Keep sparse buckets sparse; factor sums include one series per event/party."""

    def __init__(self, bars, instruments, start, end, *, factor_policy="full-state"):
        if factor_policy not in {"full-state", "national-office"}:
            raise SchemaMismatch("SEMANTIC_HISTORY_FACTOR_POLICY_INVALID")
        self.factor_policy = factor_policy
        self.instruments = instruments
        self.start, self.end = start, end
        self.prices = {
            eid: {
                int(t.timestamp()) + STEP: v
                for t, v in bars[m["market_id"]].items()
                if v is not None
            }
            for eid, m in instruments.items()
        }
        self.changes = {
            eid: {t: float(v - values[t - STEP]) for t, v in values.items() if t - STEP in values}
            for eid, values in self.prices.items()
        }
        self.reps: dict = {}
        self.sums: dict = defaultdict(lambda: [0.0, 0])
        for eid, item in sorted(instruments.items()):
            key = item["party"], item["event_id"]
            if key in self.reps:
                raise SchemaMismatch("SEMANTIC_HISTORY_DUPLICATE_EVENT_PARTY")
            self.reps[key] = eid
            for t, value in self.changes[eid].items():
                for kind, group in [
                    ("national", None),
                    ("state", item["state"]),
                    ("office", office(item)),
                ]:
                    total = self.sums[item["party"], kind, group, t]
                    total[0] += value
                    total[1] += 1

    def factor(self, target, t, excluded, kind, group):
        party = self.instruments[target]["party"]
        total, count = self.sums.get((party, kind, group, t), (0.0, 0))
        for event in excluded:
            eid = self.reps.get((party, event))
            if eid is None or t not in self.changes[eid]:
                continue
            item = self.instruments[eid]
            if (
                kind == "state"
                and item["state"] != group
                or kind == "office"
                and office(item) != group
            ):
                continue
            total -= self.changes[eid][t]
            count -= 1
        return (total / count if count else None), count

    def rows(self, source, target, extra=None):
        excluded = {self.instruments[eid]["event_id"] for eid in (source, target, extra) if eid}
        target_item = self.instruments[target]
        result = []
        missing: Counter[str] = Counter()
        for t in range(self.start + 3 * STEP, self.end, STEP):
            required = [source, target] + ([extra] if extra else [])
            if any(t not in self.changes[eid] for eid in required):
                missing["missing_source_or_target_current_previous_bucket"] += 1
                continue
            if t - STEP not in self.changes[target] or t + STEP not in self.prices[target]:
                missing["missing_target_history_or_next_bucket"] += 1
                continue
            national, n = self.factor(target, t, excluded, "national", None)
            state, s = self.factor(target, t, excluded, "state", target_item["state"])
            position, o = self.factor(target, t, excluded, "office", office(target_item))
            if (
                n < PROTOCOL["minimum_national_events"]
                or not o
                or self.factor_policy == "full-state"
                and not s
            ):
                missing["insufficient_leave_event_out_factor_coverage"] += 1
                continue
            result.append(
                {
                    "at": t,
                    "label_at": t + STEP,
                    "x": [self.changes[target][t], self.changes[target][t - STEP], national]
                    + ([state] if self.factor_policy == "full-state" else [])
                    + [position],
                    "source_delta": self.changes[source][t],
                    "control_delta": self.changes[extra][t] if extra else None,
                    "y": float(self.prices[target][t + STEP] - self.prices[target][t]),
                    "innovation": self.changes[source][t] != 0 or self.changes[target][t] != 0,
                    "state_complete": s >= PROTOCOL["minimum_complete_state_events"],
                }
            )
        return result, dict(missing)

    def raw_rows(self, source, target):
        """Observed price relationships before requiring complete model features."""
        return [
            {
                "at": t,
                "label_at": t + STEP,
                "source_delta": self.changes[source][t],
                "target_change": self.changes[target][t],
                "y": float(self.prices[target][t + STEP] - self.prices[target][t]),
            }
            for t in range(self.start + 2 * STEP, self.end, STEP)
            if t in self.changes[source]
            and t in self.changes[target]
            and t + STEP in self.prices[target]
        ]


def time_split(rows, start, end):
    # All relations and controls use the same calendar boundaries, independent of missingness.
    validation_at = start + int((end - start) * 0.6 // STEP) * STEP
    test_at = start + int((end - start) * 0.8 // STEP) * STEP
    train = [r for r in rows if r["label_at"] < validation_at - STEP]
    validation = [
        r for r in rows if validation_at + STEP <= r["at"] and r["label_at"] < test_at - STEP
    ]
    test = [r for r in rows if r["at"] >= test_at + STEP]
    return train, validation, test


def evidence(rows, differences, seed):
    blocks = [(r, d) for r, d in zip(rows, differences, strict=True) if r["innovation"]]
    if not blocks:
        return {
            "change_blocks": 0,
            "change_dates": 0,
            "ci_lower": None,
            "ci_upper": None,
            "p_value": None,
        }
    dates = defaultdict(list)
    for row, value in blocks:
        dates[datetime.fromtimestamp(row["at"], timezone.utc).date().isoformat()].append(value)
    grouped = list(dates.values())
    rng = random.Random(seed)
    draws = sorted(
        statistics.fmean(
            v for day in rng.choices(grouped, k=len(grouped)) for v in rng.choices(day, k=len(day))
        )
        for _ in range(PROTOCOL["bootstrap_draws"])
    )
    totals = [sum(day) for day in grouped]
    observed = sum(totals)
    # Exact sign enumeration with <=16 UTC dates; no false precision from 399 Monte Carlo draws.
    if len(totals) <= 16:
        signs = itertools.product((-1, 1), repeat=len(totals))
        p = sum(
            sum(s * v for s, v in zip(sign, totals, strict=True)) >= observed for sign in signs
        ) / 2 ** len(totals)
    else:
        p = (
            1 + sum(sum(rng.choice((-1, 1)) * v for v in totals) >= observed for _ in range(9999))
        ) / 10000
    return {
        "change_blocks": len(blocks),
        "change_dates": len(dates),
        "ci_lower": draws[int(len(draws) * 0.025)],
        "ci_upper": draws[min(len(draws) - 1, int(len(draws) * 0.975))],
        "p_value": p,
        "unit": "UTC_DATE_THEN_OBSERVED_5M_CHANGE_BUCKET_NOT_INDEPENDENT_NEWS",
    }


def augmented_model(train, field):
    x = [r["x"] for r in train]
    innovation = ridge(x, [r[field] for r in train], PROTOCOL["ridge_penalty"])

    def features(row):
        return [*row["x"], row[field] - predict(innovation, row["x"])]

    model = ridge([features(r) for r in train], [r["y"] for r in train], PROTOCOL["ridge_penalty"])
    return lambda row: predict(model, features(row)), model


def mse(values):
    return statistics.fmean(values) if values else None


def correlation(x, y):
    if len(x) < 2 or not statistics.pstdev(x) or not statistics.pstdev(y):
        return None
    return statistics.correlation(x, y)


def descriptive(series, edge, controls):
    """No significance or admission inferred from correlations on sparse trade buckets."""
    source, target = edge["source"], edge["target"]
    raw = series.raw_rows(source, target)
    parts = time_split(raw, series.start, series.end)
    result = {
        "edge_id": edge["edge_id"],
        "source": source,
        "target": target,
        "scope": "DESCRIPTIVE_ONLY_NOT_VALIDATED_LEADING_DIRECTION",
        "periods": {},
    }
    for name, rows in zip(("all", "train", "validation", "test"), (raw, *parts), strict=True):
        current = {
            "observations": len(rows),
            "source_change_buckets": sum(r["source_delta"] != 0 for r in rows),
            "target_future_change_buckets": sum(r["y"] != 0 for r in rows),
            "utc_dates": len({datetime.fromtimestamp(r["at"], timezone.utc).date() for r in rows}),
            "same_bucket_correlation": correlation(
                [r["source_delta"] for r in rows], [r["target_change"] for r in rows]
            ),
            "source_to_next_target_correlation": correlation(
                [r["source_delta"] for r in rows], [r["y"] for r in rows]
            ),
        }
        matched = [r for r in rows if r["at"] + STEP in series.changes[source]]
        current["future_source_placebo"] = {
            "observations": len(matched),
            "forward_on_identical_mask": correlation(
                [r["source_delta"] for r in matched], [r["y"] for r in matched]
            ),
            "future_source_to_current_target": correlation(
                [series.changes[source][r["at"] + STEP] for r in matched],
                [r["target_change"] for r in matched],
            ),
            "future_data_used_for_placebo_only": True,
        }
        comparisons = []
        for control in controls:
            shared = [r for r in rows if r["at"] in series.changes[control]]
            comparisons.append(
                {
                    "control_source": control,
                    "matched_observations": len(shared),
                    "candidate_correlation": correlation(
                        [r["source_delta"] for r in shared], [r["y"] for r in shared]
                    ),
                    "random_correlation": correlation(
                        [series.changes[control][r["at"]] for r in shared], [r["y"] for r in shared]
                    ),
                }
            )
        current["random_controls_on_identical_times"] = comparisons
        result["periods"][name] = current
    return result


def score_rows(train, validation, test):
    baseline = ridge([r["x"] for r in train], [r["y"] for r in train], PROTOCOL["ridge_penalty"])
    own = ridge([r["x"][:2] for r in train], [r["y"] for r in train], PROTOCOL["ridge_penalty"])
    forecast, model = augmented_model(train, "source_delta")
    errors = [(r["y"] - forecast(r)) ** 2 for r in test]
    base_errors = [(r["y"] - predict(baseline, r["x"])) ** 2 for r in test]
    differences = [a - b for a, b in zip(base_errors, errors, strict=True)]
    source_model = ridge(
        [r["x"] for r in train], [r["source_delta"] for r in train], PROTOCOL["ridge_penalty"]
    )
    validation_increment = mse(
        [
            (r["y"] - predict(baseline, r["x"])) ** 2 - (r["y"] - forecast(r)) ** 2
            for r in validation
        ]
    )
    return (
        {
            "no_change_mse": mse([r["y"] ** 2 for r in test]),
            "target_history_mse": mse([(r["y"] - predict(own, r["x"][:2])) ** 2 for r in test]),
            "factor_baseline_mse": mse(base_errors),
            "semantic_mse": mse(errors),
            "test_increment": mse(differences),
            "validation_increment": validation_increment,
            "test_half_increments": [
                mse(differences[: len(test) // 2]),
                mse(differences[len(test) // 2 :]),
            ],
            "source_coefficient_per_price_unit": model["coefficients"][-1] / model["scales"][-1],
            "raw_source_to_next_target_correlation": correlation(
                [r["source_delta"] for r in test], [r["y"] for r in test]
            ),
            "raw_same_bucket_correlation": correlation(
                [r["source_delta"] for r in test], [r["x"][0] for r in test]
            ),
            "controlled_source_to_next_target_correlation": correlation(
                [r["source_delta"] - predict(source_model, r["x"]) for r in test],
                [r["y"] - predict(baseline, r["x"]) for r in test],
            ),
        },
        differences,
        errors,
        base_errors,
    )


def enough(parts):
    return all(
        len(rows) >= PROTOCOL[key]
        for rows, key in zip(
            parts,
            ("minimum_train_rows", "minimum_validation_rows", "minimum_test_rows"),
            strict=True,
        )
    )


def control_graphs(instruments, candidates):
    # Preserve each target's incoming degree, not the sources' outgoing-degree distribution.
    result: dict[str, list[str]] = {e["edge_id"]: [] for e in candidates}
    for target in sorted({e["target"] for e in candidates}):
        item = instruments[target]
        incoming = [e for e in candidates if e["target"] == target]
        choices = [
            eid
            for eid, m in sorted(instruments.items())
            if m["party"] == item["party"] and m["state"] != item["state"]
        ]
        if len(choices) < len(incoming):
            raise SchemaMismatch("SEMANTIC_HISTORY_RANDOM_DEGREE_CONTROL_UNAVAILABLE")
        for graph in range(PROTOCOL["random_controls"]):
            shuffled = list(choices)
            random.Random(f"SEMANTIC-HISTORY-CONTROLS-1:{graph}:{target}").shuffle(shuffled)
            for edge, source in zip(incoming, shuffled, strict=False):
                result[edge["edge_id"]].append(source)
    return result


def fit_relation(series, edge, controls):
    rows, missing = series.rows(edge["source"], edge["target"])
    parts = time_split(rows, series.start, series.end)
    result = {
        **edge,
        "missing": missing,
        "rows": dict(zip(("train", "validation", "test"), map(len, parts), strict=True)),
        "p_value": None,
        "fdr_q": None,
        "predictive_increment_supported": False,
        "factor_policy": series.factor_policy,
        "limitations": (
            ["STATE_SPECIFIC_COMMON_NEWS_NOT_SEPARATELY_IDENTIFIED"]
            if series.factor_policy == "national-office"
            else []
        ),
        "state_factor_coverage": state_factor_coverage(
            series.instruments, edge, PROTOCOL["minimum_complete_state_events"]
        ),
    }
    if not enough(parts):
        return {**result, "status": "INSUFFICIENT_CONTIGUOUS_TRADE_WINDOWS"}, [], []
    train, validation, test = parts
    scores, differences, errors, base_errors = score_rows(*parts)
    result.update(
        status="FITTED",
        scores=scores,
        evidence=evidence(test, differences, edge["edge_id"]),
        all_state_factors_complete=all(r["state_complete"] for r in rows),
        test_nonzero_labels=sum(r["y"] != 0 for r in test),
        train_source_changes=sum(r["source_delta"] != 0 for r in train),
    )
    result["p_value"] = result["evidence"]["p_value"]
    control_results = []
    for source in controls:
        matched, omitted = series.rows(edge["source"], edge["target"], source)
        matched_parts = time_split(matched, series.start, series.end)
        comparison = {
            "edge_id": edge["edge_id"],
            "control_source": source,
            "matched_rows": list(map(len, matched_parts)),
            "missing": omitted,
            "matched_time_hashes": [
                content_hash([r["at"] for r in part]) for part in matched_parts
            ],
            "identical_train_validation_test_times": True,
            "leave_out_events": sorted(
                {
                    series.instruments[e]["event_id"]
                    for e in (source, edge["source"], edge["target"])
                }
            ),
        }
        if not enough(matched_parts):
            comparison["status"] = "INSUFFICIENT_MATCHED_WINDOWS"
        else:
            tr, _, te = matched_parts
            semantic, _ = augmented_model(tr, "source_delta")
            random_source, _ = augmented_model(tr, "control_delta")
            sem_errors = [(r["y"] - semantic(r)) ** 2 for r in te]
            random_errors = [(r["y"] - random_source(r)) ** 2 for r in te]
            comparison.update(
                status="MATCHED",
                semantic_mse=mse(sem_errors),
                random_mse=mse(random_errors),
                semantic_advantage=mse(
                    [a - b for a, b in zip(random_errors, sem_errors, strict=True)]
                ),
            )
        control_results.append(comparison)
    result["random_control_count"] = sum(c["status"] == "MATCHED" for c in control_results)
    result["beats_all_matched_random_controls"] = result["random_control_count"] == PROTOCOL[
        "random_controls"
    ] and all(c.get("semantic_advantage", 0) > 0 for c in control_results)
    observations = [
        {
            "edge_id": edge["edge_id"],
            "at": r["at"],
            "label_at": r["label_at"],
            "actual_next_vwap_change": r["y"],
            "source_change": r["source_delta"],
            "change_bucket": r["innovation"],
            "baseline_squared_error": b,
            "semantic_squared_error": s,
        }
        for r, b, s in zip(test, base_errors, errors, strict=True)
    ]
    return result, control_results, observations


def classify(results):
    # Missing/unfitted prespecified hypotheses remain in the family as p=1, not silently dropped.
    family = [{"p_value": r["p_value"] if r["p_value"] is not None else 1.0} for r in results]
    fdr(family)
    dependence_penalty = sum(1 / i for i in range(1, len(results) + 1))
    for row, adjusted in zip(results, family, strict=True):
        structural_reasons = (
            ["STATE_FACTOR_STRUCTURALLY_UNAVAILABLE"]
            if row.get("state_factor_coverage", {}).get("status") == "STRUCTURALLY_UNAVAILABLE"
            and row.get("factor_policy", "full-state") == "full-state"
            else []
        )
        if row["p_value"] is not None:
            row["fdr_q"] = adjusted["fdr_q"]
            row["fdr_q_arbitrary_dependence"] = min(1.0, adjusted["fdr_q"] * dependence_penalty)
        if row["status"] != "FITTED":
            row["reasons"] = [row["status"], *structural_reasons]
            continue
        e, s = row["evidence"], row["scores"]
        reasons = structural_reasons
        if (
            e["change_blocks"] < PROTOCOL["minimum_test_change_blocks"]
            or e["change_dates"] < PROTOCOL["minimum_test_change_dates"]
        ):
            reasons.append("INSUFFICIENT_CHANGE_BUCKETS_OR_DATES")
        if (
            row.get("factor_policy", "full-state") == "full-state"
            and not row["all_state_factors_complete"]
        ):
            reasons.append("STATE_FACTOR_CONTROL_INCOMPLETE")
        if not row["train_source_changes"]:
            reasons.append("NO_TRAINING_SOURCE_PRICE_CHANGES")
        if not (
            s["validation_increment"] > 0
            and s["test_increment"] > 0
            and all(v is not None and v > 0 for v in s["test_half_increments"])
            and e["ci_lower"] is not None
            and e["ci_lower"] > 0
            and row["fdr_q"] is not None
            and row["fdr_q_arbitrary_dependence"] <= PROTOCOL["fdr_alpha"]
        ):
            reasons.append("ROBUST_INCREMENT_NOT_SUPPORTED")
        if not row["beats_all_matched_random_controls"]:
            reasons.append("MATCHED_RANDOM_CONTROL_NOT_BEATEN_OR_UNAVAILABLE")
        row["reasons"] = reasons
        row["predictive_increment_supported"] = not reasons


def csv_write(path, rows):
    if not rows:
        Path(path).write_text("")
        return
    with Path(path).open("w", newline="") as stream:
        writer = csv.DictWriter(stream, list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def research(study, registry_path, previous_model, output, *, factor_policy="full-state"):
    root = Path(output)
    root.mkdir(parents=True, exist_ok=True)
    if any(
        (root / name).exists()
        for name in (
            "protocol.json",
            "relation_diagnostics.json",
            "SUMMARY.json",
            "raw_relation_diagnostics.json",
        )
    ):
        raise SchemaMismatch("SEMANTIC_HISTORY_OUTPUT_ALREADY_FROZEN_USE_NEW_DIRECTORY")
    registry = json.loads(Path(registry_path).read_text())
    _, instruments, registry_hash = metadata(registry_path)
    if any(eid.startswith("SIM-") for eid in instruments):
        raise SchemaMismatch("SEMANTIC_HISTORY_REAL_DATA_REQUIRED")
    prior = json.loads(Path(previous_model).read_text())
    candidates = edges(instruments, set(instruments))
    old_edges = {r["edge_id"] for r in prior["relations"]}
    if old_edges - {e["edge_id"] for e in candidates}:
        raise SchemaMismatch("SEMANTIC_HISTORY_PRIMARY_COHORT_CHANGED")
    combined_path = Path(study) / "COMBINED_REAL_READ.json"
    combined_bytes = combined_path.read_bytes()
    combined = json.loads(combined_bytes)
    start, end = [
        int(stamp(combined[k]).timestamp())
        for k in ("window_from_inclusive", "window_to_exclusive")
    ]
    control_sources = control_graphs(instruments, candidates)
    protocol = {
        **PROTOCOL,
        "frozen_at": datetime.now(timezone.utc).isoformat(),
        "start": start,
        "end": end,
        "registry_sha256": registry_hash,
        "history_combined_sha256": hashlib.sha256(combined_bytes).hexdigest(),
        "previous_model_sha256": hashlib.sha256(Path(previous_model).read_bytes()).hexdigest(),
        "code_sha256": {
            str(p): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in (
                Path(__file__),
                Path("src/sigbot/history_ssr.py"),
                Path("src/sigbot/history.py"),
                Path("src/sigbot/strategies/semantic_sig_v1/model.py"),
                Path("src/sigbot/strategies/semantic_sig_v1/timeseries.py"),
            )
        },
        "fdr_family": "ALL_PRESPECIFIED_HYPOTHESES_BH_AND_ARBITRARY_DEPENDENCE_CORRECTION",
        "random_graph_scope": "TARGET_IN_DEGREE_PRESERVED_SOURCE_OUT_DEGREE_NOT_PRESERVED",
        "hypotheses": [
            {**e, "primary": e["edge_id"] in old_edges, "controls": control_sources[e["edge_id"]]}
            for e in candidates
        ],
    }
    if factor_policy not in {"full-state", "national-office"}:
        raise SchemaMismatch("SEMANTIC_HISTORY_FACTOR_POLICY_INVALID")
    protocol["factor_policy"] = factor_policy
    if factor_policy == "national-office":
        protocol.update(
            version="S3_TRADE_VWAP_3_FIXED_NATIONAL_OFFICE_BASELINE",
            features=[v for v in PROTOCOL["features"] if v != "state"],
            minimum_complete_state_events=None,
            amendment="NEW_EXPLORATORY_PROTOCOL_FIXED_FEATURE_SET_NO_STATE_FACTOR_NO_ZERO_IMPUTATION",
            identification_scope="PREDICTIVE_INCREMENT_OVER_AVAILABLE_CONTROLS_NOT_STATE_SPECIFIC_CAUSALITY",
        )
    write_json(root / "protocol.json", protocol)
    write_json(root / "protocol_hash.json", {"sha256": content_hash(protocol)})
    bars, bar_sources, audit = load_history(study, registry)
    if (int(stamp(audit["start"]).timestamp()), int(stamp(audit["end"]).timestamp())) != (
        start,
        end,
    ):
        raise SchemaMismatch("SEMANTIC_HISTORY_WINDOW_CHANGED")
    audit.update(
        registry_sha256=registry_hash,
        protocol_sha256=content_hash(protocol),
        history_combined_sha256=hashlib.sha256(combined_path.read_bytes()).hexdigest(),
        historical_available_at="UNKNOWN_POST_HOC_RETRIEVAL",
        executable_orderbook=False,
    )
    if audit["history_combined_sha256"] != protocol["history_combined_sha256"]:
        raise SchemaMismatch("SEMANTIC_HISTORY_MANIFEST_CHANGED_DURING_RUN")
    write_json(root / "input_audit.json", audit)
    csv_write(
        root / "bar_sources.csv",
        [
            {
                "market_id": mid,
                "bucket_start": t.isoformat(),
                "assumed_bucket_complete_at": int(t.timestamp()) + STEP,
                "raw_response_sha256": sha,
            }
            for (mid, t), sha in sorted(bar_sources.items())
        ],
    )
    series = TradeSeries(bars, instruments, start, end, factor_policy=factor_policy)
    csv_write(
        root / "market_coverage.csv",
        [
            {
                "exchange_id": eid,
                "market_id": m["market_id"],
                "event_id": m["event_id"],
                "state": m["state"],
                "party": m["party"],
                "observed_vwap_buckets": len(series.prices[eid]),
                "contiguous_price_changes": len(series.changes[eid]),
                "nonzero_price_changes": sum(v != 0 for v in series.changes[eid].values()),
                "possible_buckets": (end - start) // STEP,
            }
            for eid, m in sorted(instruments.items())
        ],
    )
    results, controls, observations, raw_diagnostics = [], [], [], []
    for edge in protocol["hypotheses"]:
        result, comparison, samples = fit_relation(
            series, {k: edge[k] for k in ("source", "target", "edge_id")}, edge["controls"]
        )
        result["primary"] = edge["primary"]
        results.append(result)
        controls.extend(comparison)
        observations.extend(samples)
        raw_result = descriptive(series, edge, edge["controls"])
        raw_result["primary"] = edge["primary"]
        raw_diagnostics.append(raw_result)
    classify(results)
    primary = [r for r in results if r["primary"]]
    summary = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "protocol_sha256": content_hash(protocol),
        "layer": "LOCAL_REAL_READ_RETROSPECTIVE_RESEARCH",
        "data_kind": PROTOCOL["price"],
        "factor_policy": factor_policy,
        "state_factor_required": factor_policy == "full-state",
        "sample_role": PROTOCOL["sample_role"],
        "directed_candidates": len(results),
        "underlying_unordered_event_pairs": len(
            {
                tuple(
                    sorted(
                        (instruments[e["source"]]["event_id"], instruments[e["target"]]["event_id"])
                    )
                )
                for e in candidates
            }
        ),
        "fitted": sum(r["status"] == "FITTED" for r in results),
        "primary_candidates": len(primary),
        "primary_fitted": sum(r["status"] == "FITTED" for r in primary),
        "descriptive_relations_with_observations": sum(
            r["periods"]["all"]["observations"] > 0 for r in raw_diagnostics
        ),
        "descriptive_relations_with_test_correlation": sum(
            r["periods"]["test"]["source_to_next_target_correlation"] is not None
            for r in raw_diagnostics
        ),
        "positive_test_increment": sum(
            r.get("scores", {}).get("test_increment", 0) > 0 for r in results
        ),
        "positive_validation_and_test": sum(
            r.get("scores", {}).get("test_increment", 0) > 0
            and r.get("scores", {}).get("validation_increment", 0) > 0
            for r in results
        ),
        "predictive_increment_supported": sum(r["predictive_increment_supported"] for r in results),
        "matched_random_comparisons": sum(c["status"] == "MATCHED" for c in controls),
        "maximum_test_change_dates": max(
            (r.get("evidence", {}).get("change_dates", 0) for r in results), default=0
        ),
        "reasons": dict(Counter(reason for r in results for reason in r["reasons"])),
        "execution_evidence": {
            "historical_bid_ask": None,
            "fees": None,
            "capacity": None,
            "cost_adjusted_return": None,
            "real_pnl": None,
        },
        "live_eligible": False,
        "real_orders": 0,
        "real_cancels": 0,
        "decision": "NEED_MORE_DATA",
        "decision_reasons": [
            "TRADED_PRICE_STUDY_HAS_NO_EXECUTABLE_COST_PROOF",
            "HISTORICAL_AVAILABILITY_AND_RULE_VINTAGE_UNKNOWN",
            "PREVIOUSLY_EXPLORED_DATA_NEEDS_FRESH_FORWARD_CONFIRMATION",
        ],
    }
    if not summary["fitted"]:
        summary.update(
            predictive_model_evaluation="NOT_EVALUATED_INSUFFICIENT_REQUIRED_WINDOWS",
            maximum_test_change_dates=None,
            positive_test_increment=None,
            positive_validation_and_test=None,
        )
        summary["decision_reasons"].insert(
            0, "NO_RELATION_WITH_REQUIRED_TRAIN_VALIDATION_TEST_WINDOWS"
        )
    else:
        summary["predictive_model_evaluation"] = "EVALUATED_RETROSPECTIVE_PRICE_ONLY"
        if not summary["predictive_increment_supported"]:
            summary["decision_reasons"].insert(0, "ROBUST_PREDICTIVE_INCREMENT_NOT_SUPPORTED")
    write_json(root / "relation_diagnostics.json", results)
    write_json(
        root / "lead_lag_validation.json",
        [
            {
                "edge_id": r["edge_id"],
                "primary": r["primary"],
                "status": r["status"],
                "forecast_horizon_seconds": STEP,
                "scores": r.get("scores"),
                "evidence": r.get("evidence"),
                "fdr_q": r["fdr_q"],
                "fdr_q_arbitrary_dependence": r.get("fdr_q_arbitrary_dependence"),
                "predictive_increment_supported": r["predictive_increment_supported"],
                "reasons": r["reasons"],
                "causal_effect_identified": False,
                "factor_policy": r["factor_policy"],
                "limitations": r["limitations"],
            }
            for r in results
        ],
    )
    write_json(root / "random_controls.json", controls)
    write_json(root / "raw_relation_diagnostics.json", raw_diagnostics)
    csv_write(
        root / "raw_relation_diagnostics.csv",
        [
            {
                "edge_id": r["edge_id"],
                "primary": r["primary"],
                "period": period,
                **{
                    k: values[k]
                    for k in (
                        "observations",
                        "source_change_buckets",
                        "target_future_change_buckets",
                        "utc_dates",
                        "same_bucket_correlation",
                        "source_to_next_target_correlation",
                    )
                },
            }
            for r in raw_diagnostics
            for period, values in r["periods"].items()
        ],
    )
    csv_write(root / "test_observations.csv", observations)
    write_json(root / "economic_capacity.json", summary["execution_evidence"])
    write_json(root / "SUMMARY.json", summary)
    write_json(root / "instruments.json", instruments)
    return summary
