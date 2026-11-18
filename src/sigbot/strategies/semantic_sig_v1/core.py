"""Decimal payoff research, capacity constraints and point-in-time research gates."""

from __future__ import annotations

import itertools
import math
from decimal import Decimal, InvalidOperation

from sigbot.domain import content_hash, utc_time
from sigbot.errors import SchemaMismatch
from sigbot.strategies.semantic_sig_v1.registry import bundles, groups, review_status


def number(value, *, positive=False, probability=False):
    if value is None or isinstance(value, (bool, float)):
        raise SchemaMismatch("SEMANTIC_EXACT_DECIMAL_REQUIRED")
    try:
        result = Decimal(value)
    except (InvalidOperation, TypeError, ValueError):
        raise SchemaMismatch("SEMANTIC_INVALID_AMOUNT") from None
    if (
        not result.is_finite()
        or result < 0
        or positive
        and result <= 0
        or probability
        and result > 1
    ):
        raise SchemaMismatch("SEMANTIC_INVALID_AMOUNT")
    return result


def payouts(legs, winners, void_refunds=None):
    """Refunds are actual per-leg refundable costs, never binary terminal payouts."""
    if not legs or not winners:
        raise SchemaMismatch("SEMANTIC_EMPTY_PAYOFF_STATES")
    if len({leg["key"] for leg in legs}) != len(legs):
        raise SchemaMismatch("SEMANTIC_DUPLICATE_LEG")
    result = {}
    for winner in winners:
        if winner in {"VOID", "N/A", "UNKNOWN"}:
            raise SchemaMismatch("SEMANTIC_NORMAL_STATE_REQUIRED")
        total = Decimal(0)
        for leg in legs:
            qty = number(leg["quantity"], positive=True)
            if leg["side"] not in {"YES", "NO"}:
                raise SchemaMismatch("SEMANTIC_SIDE_INVALID")
            if void_refunds is not None and leg["key"] in void_refunds:
                total += number(void_refunds[leg["key"]])
            else:
                affiliations = winner if isinstance(winner, frozenset) else frozenset([winner])
                yes = Decimal(int(leg["party"] in affiliations))
                total += qty * (yes if leg["side"] == "YES" else 1 - yes)
        label = "|".join(sorted(winner)) if isinstance(winner, frozenset) else winner
        result[label] = total
    return result


def sweep(levels, quantity):
    """Buy through documented ask levels. Own or unattributed liquidity is excluded."""
    left, cost, fills = number(quantity, positive=True), Decimal(0), []
    parsed = [(number(x["price"], probability=True), number(x["quantity"])) for x in levels]
    if parsed != sorted(parsed) or len({p for p, _ in parsed}) != len(parsed):
        raise SchemaMismatch("SEMANTIC_BOOK_ORDER_OR_DUPLICATE_PRICE")
    for level, (price, depth) in zip(levels, parsed, strict=True):
        if level.get("own") is not False:
            raise SchemaMismatch("SEMANTIC_OWN_LIQUIDITY_UNKNOWN")
        take = min(left, depth)
        if take:
            fills.append({"price": str(price), "quantity": str(take)})
            cost += price * take
            left -= take
        if not left:
            break
    return None if left else {"cost": cost, "fills": fills}


def scan(
    markets,
    books,
    rules,
    reviews,
    *,
    at,
    quantity,
    fees=None,
    cash=None,
    buffer="0",
    max_age=5,
    max_skew=2,
    test_only=False,
    candidate_audits=None,
):
    qty, safety = number(quantity, positive=True), number(buffer)
    if not math.isfinite(max_age) or not math.isfinite(max_skew) or max_age < 0 or max_skew < 0:
        raise SchemaMismatch("SEMANTIC_INVALID_TIME_TOLERANCE")
    if test_only and any(not m.get("sig_market_id", "").startswith("SIM-") for m in markets):
        raise SchemaMismatch("SEMANTIC_TEST_NAMESPACE_REQUIRED")
    reports = []
    for race_id, members in groups(markets).items():
        review = reviews.get(race_id)
        status, rule_reasons = review_status(members, rules, review, at)
        for subset in bundles(members):
            keys = [m["local_market_key"] for m in subset]
            opportunity_id = "S1:" + content_hash([race_id, sorted(keys), "BUY_NO"])
            row = {
                "opportunity_id": opportunity_id,
                "generated_at": at.isoformat(),
                "race_id": race_id,
                "market_keys": keys,
                "relation_status": status,
                "relation_version": content_hash([rules, review]) if status == "VERIFIED" else None,
                "evaluation": "NOT_EVALUATED",
                "rejection_reasons": list(rule_reasons),
                "quantity": str(qty),
                "conditional_min_terminal_pnl": None,
                "normal_payouts": None,
                "normal_min_payout": None,
                "payoff_evaluation": "NOT_EVALUATED",
                "payoff_state_scope": None,
                "exception_min_pnl": None,
                "entry_cost": None,
                "observed_entry_cost": None,
                "candidate_conditioned_min_payout": None,
                "candidate_conditioned_payouts": None,
                "entry_cost_data_reasons": [],
                "fees": None,
                "lock_duration_seconds": None,
                "realized_pnl": None,
                "trade_enabled": False,
                "legs_atomic": False,
                "layer": "MOCK" if test_only else "REAL_READ",
                "liquidity_allocation": "ALTERNATIVES_NOT_SUMMABLE",
                "input_evidence": [],
            }
            legs = [
                {
                    "key": m["local_market_key"],
                    "party": m["party_code"],
                    "side": "NO",
                    "quantity": str(qty),
                }
                for m in subset
            ]
            normal, winners = None, None
            # A payment bound needs no price history, cash allocation or execution fee.
            # Without an approved exclusion proof, include every binary outcome vector.
            # These are conservative scenarios, not claims about actual fusion nominees.
            if not set(rule_reasons) & {
                "ID_MAPPING_UNVERIFIED",
                "MISSING_CONTRACT_INFO_RULES",
                "RULE_SCOPE_MISMATCH",
                "RULE_NOT_YET_AVAILABLE",
            }:
                parties = sorted({m["party_code"] for m in members})
                if status == "VERIFIED":
                    winners = [*parties, "OTHER"]
                    scope = "REVIEWED_MUTUAL_EXCLUSION_WITH_OTHER"
                else:
                    winners = ["OTHER"] + [
                        frozenset(combination)
                        for size in range(1, len(parties) + 1)
                        for combination in itertools.combinations(parties, size)
                    ]
                    scope = "CONSERVATIVE_ALL_BINARY_STATES_NOT_CANDIDATE_FACTS"
                normal = payouts(legs, winners)
                row.update(
                    normal_payouts={k: str(v) for k, v in normal.items()},
                    normal_min_payout=str(min(normal.values())),
                    payoff_evaluation="EVALUATED_NORMAL_STATES_ONLY",
                    payoff_state_scope=scope,
                )
            candidate = (candidate_audits or {}).get(race_id)
            if candidate:
                row["candidate_evidence"] = {
                    k: v
                    for k, v in candidate.items()
                    if k not in {"candidates", "listed_affiliation_states"}
                }
                if candidate["status"] == "CONDITIONAL_SNAPSHOT_VERIFIED" and normal is not None:
                    conditional = payouts(
                        legs, [frozenset(v) for v in candidate["listed_affiliation_states"]]
                    )
                    row.update(
                        candidate_conditioned_payouts={k: str(v) for k, v in conditional.items()},
                        candidate_conditioned_min_payout=str(min(conditional.values())),
                        candidate_payoff_scope="LISTED_BALLOT_LABELS_ONLY_NOT_ALL_SIG_AFFILIATIONS",
                    )
            costs, timestamps = [], []
            for market in subset:
                book = books.get(market["exchange_id"])
                if book is None:
                    row["rejection_reasons"].append("MISSING_REAL_BOOK")
                    continue
                row["input_evidence"].append(
                    {
                        "exchange_id": market["exchange_id"],
                        "received_at": book.get("received_at"),
                        "asOf": book.get("asOf"),
                        "raw_sha256": book.get("raw_sha256"),
                        "data_provenance": book.get("data_provenance"),
                        "validation_received_at": book.get("validation_received_at"),
                        "rest_validation_sequence": book.get("rest_validation_sequence"),
                        "own_order_evidence": book.get("own_order_evidence"),
                    }
                )
                expected_provenance = "SYNTHETIC" if test_only else "OFFICIAL_REAL"
                if book.get("data_provenance") != expected_provenance:
                    row["rejection_reasons"].append("NON_REAL_DATA_FOR_RESEARCH")
                if book.get("quality") != "HEALTHY":
                    row["rejection_reasons"].append("BOOK_DATA_GAP")
                if book.get("marketId") != market["sig_market_id"]:
                    row["rejection_reasons"].append("BOOK_MARKET_ID_MISMATCH")
                stamp = book.get("received_at")
                validated = book.get("validation_received_at")
                # An unchanged, freshly REST-validated version is not a stale book.
                # Keep the original receive time and require the exact engine sequence.
                if (
                    stamp
                    and validated
                    and book.get("quality") == "HEALTHY"
                    and type(book.get("rest_validation_sequence")) is int
                    and book["rest_validation_sequence"] == (book.get("asOf") or {}).get("sequence")
                    and utc_time(stamp) <= utc_time(validated) <= at
                    and book.get("validation_requested_at")
                    and utc_time(stamp)
                    <= utc_time(book["validation_requested_at"])
                    <= utc_time(validated)
                ):
                    stamp = validated
                if not stamp or not 0 <= (at - utc_time(stamp)).total_seconds() <= max_age:
                    row["rejection_reasons"].append("BOOK_STALE_OR_FUTURE")
                else:
                    timestamps.append(utc_time(stamp))
                if not book.get("own_orders_known"):
                    row["rejection_reasons"].append("OWN_LIQUIDITY_UNKNOWN")
                levels = book.get("no_asks")
                if levels is None:
                    row["rejection_reasons"].append("NO_SIDE_MAPPING_UNVERIFIED")
                elif book.get("own_orders_known"):
                    try:
                        cost = sweep(levels, qty)
                    except SchemaMismatch as exc:
                        # Reject the affected bundle explicitly; other books remain usable.
                        row["rejection_reasons"].append(exc.code)
                    else:
                        if cost is None:
                            row["rejection_reasons"].append("INSUFFICIENT_DEPTH")
                        else:
                            costs.append(cost)
            if timestamps and (max(timestamps) - min(timestamps)).total_seconds() > max_skew:
                row["rejection_reasons"].append("ASOF_JOIN_SKEW_EXCEEDED")
            # Observe the price of the specified legs separately from a payoff or
            # net-return claim. Missing fees/cash/review must not erase valid depth.
            data_reasons = row["rejection_reasons"][len(rule_reasons) :]
            if any(m.get("mapping_status") != "EXACT_OFFICIAL_TITLE_AND_EXCHANGE" for m in subset):
                data_reasons = [*data_reasons, "ID_MAPPING_UNVERIFIED"]
            row["entry_cost_data_reasons"] = sorted(set(data_reasons))
            if not data_reasons and len(costs) == len(subset):
                row["observed_entry_cost"] = str(sum((c["cost"] for c in costs), Decimal(0)))
            if fees is None or not review or not review.get("fee_evidence"):
                row["rejection_reasons"].append("FEES_UNVERIFIED")
            if cash is None:
                row["rejection_reasons"].append("CASH_BUDGET_UNSPECIFIED")
            row["rejection_reasons"] = sorted(set(row["rejection_reasons"]))
            if not row["rejection_reasons"]:
                cost = sum((x["cost"] for x in costs), Decimal(0))
                assert normal is not None and winners is not None
                fee = number(fees)
                pnl = min(normal.values()) - cost - fee
                exceptions = {}
                for count in range(1, len(legs) + 1):
                    for cancelled in itertools.combinations(range(len(legs)), count):
                        refunds = {legs[i]["key"]: costs[i]["cost"] for i in cancelled}
                        refunded = payouts(legs, winners, refunds)
                        name = "|".join(sorted(refunds)) + ":CANCELLED"
                        exceptions[name] = str(min(refunded.values()) - cost - fee)
                row.update(
                    evaluation="EVALUATED_CONDITIONAL_NORMAL_ONLY",
                    entry_cost=str(cost),
                    fees=str(fee),
                    normal_payouts={k: str(v) for k, v in normal.items()},
                    conditional_min_terminal_pnl=str(pnl),
                    exception_min_pnl=str(min(Decimal(v) for v in exceptions.values())),
                    exception_scenarios=exceptions,
                    legs_depth=costs,
                )
                if cost + fee > number(cash):
                    row["rejection_reasons"].append("CASH_BUDGET_EXCEEDED")
                if pnl <= safety:
                    row["rejection_reasons"].append("NO_COST_ADJUSTED_NORMAL_EDGE")
                row["risk_review_required"] = True
                row["candidate_only"] = not row["rejection_reasons"]
            reports.append(row)
    return reports


def asof(records, at, tolerance_seconds):
    """Receipt time determines visibility. No unlimited forward fill."""
    if tolerance_seconds < 0:
        raise ValueError("negative tolerance")
    available = [r for r in records if utc_time(r["received_at"]) <= at]
    if not available:
        return None
    item = max(available, key=lambda r: utc_time(r["received_at"]))
    return (
        item
        if item.get("quality") == "HEALTHY"
        and (at - utc_time(item["received_at"])).total_seconds() <= tolerance_seconds
        else None
    )


def leave_target_out(records, target_race):
    """Exclude every party and mechanical reverse side of the underlying target."""
    unique = {}
    for item in records:
        if item["race_id"] == target_race:
            continue
        key = item["race_id"], item["received_at"]
        if key in unique:
            raise SchemaMismatch("SEMANTIC_FACTOR_REQUIRES_RACE_LEVEL_DEDUP")
        unique[key] = item
    return list(unique.values())


def poisson_binomial(probabilities):
    """Independent baseline only; caller supplies all real probabilities explicitly."""
    distribution = [Decimal(1)]
    for value in probabilities:
        p = number(value, probability=True)
        updated = [Decimal(0)] * (len(distribution) + 1)
        for index, mass in enumerate(distribution):
            updated[index] += mass * (1 - p)
            updated[index + 1] += mass * p
        distribution = updated
    return distribution


def seat_coverage(markets):
    races = groups(markets)
    senate = [k for k, v in races.items() if v[0]["office"] == "SENATE"]
    house = [k for k, v in races.items() if v[0]["office"] == "HOUSE"]
    return {
        "senate_listed_races": len(senate),
        "ohio_present": "2026-OH-SENATE" in senate,
        "house_listed_districts": len(house),
        "house_unlisted_districts": 435 - len(house),
        "coverage_status": "NOT_EVALUATED",
        "national_replication_verified": False,
        "control_probabilities": None,
        "pivotalities": None,
        "reasons": [
            "COMPLETE_SEAT_BASELINE_AND_SPECIAL_ELECTIONS_REQUIRED",
            "INDEPENDENT_CAUCUS_UNKNOWN",
            "CONTROL_RULES_UNVERIFIED",
            "DISTRICT_BOUNDARY_VERSIONS_REQUIRED",
        ],
    }
