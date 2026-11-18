"""Multi-event PRC evaluator and one deterministic account-wide allocator."""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from decimal import ROUND_FLOOR, Decimal
from statistics import median
from typing import Literal, NoReturn

from sigbot.domain import ZERO
from sigbot.errors import RiskRejected
from sigbot.strategies.prc_sig_v1.models import (
    POLICY,
    Book,
    Candidate,
    Exposure,
    PollQuestion,
    PollReport,
    Snapshot,
)
from sigbot.strategies.prc_sig_v1.poll_math import paired_poll_z, price_gate


def reject(reason: str) -> NoReturn:
    raise RiskRejected("PRC_" + reason)


def floor(quantity: Decimal, lot: Decimal) -> Decimal:
    return (quantity / lot).to_integral_value(rounding=ROUND_FLOOR) * lot


def consume(levels, quantity: Decimal, buy: bool) -> tuple[Decimal, Decimal]:
    if quantity <= 0:
        reject("INVALID_QUANTITY")
    ordered = sorted(levels, key=lambda x: x.price, reverse=not buy)
    remaining, cost, worst = quantity, ZERO, ZERO
    for level in ordered:
        taken = min(remaining, level.quantity)
        cost += taken * level.price
        remaining -= taken
        worst = level.price
        if remaining == 0:
            return cost / quantity, worst
    reject("FULL_QUANTITY_DEPTH_MISSING")
    raise AssertionError("unreachable")


def validate_book(book: Book, now: datetime):
    if not book.bids or not book.asks:
        reject("BOOK_SIDE_MISSING")
    if not book.gap_free or not book.own_orders_known or not book.version:
        reject("BOOK_GAP_OR_OWN_ORDER_UNKNOWN")
    if not book.source_at <= book.received_at <= book.verified_at <= now:
        reject("BOOK_TIME_INVALID")
    if (now - book.source_at).total_seconds() > 5 or (now - book.verified_at).total_seconds() > 5:
        reject("BOOK_STALE")
    bid, ask = max(x.price for x in book.bids), min(x.price for x in book.asks)
    if bid > ask:
        reject("CROSSED_BOOK")
    return bid, ask


def window_medians(
    frames, t0: datetime, now: datetime
) -> tuple[Decimal, Decimal, Decimal, Decimal]:
    buckets: dict[int, list[Decimal]] = {}
    seen = set()
    for frame in frames:
        if frame.at in seen:
            reject("DUPLICATE_QUOTE_TIME")
        seen.add(frame.at)
        index = int((frame.at - t0).total_seconds() // 60)
        end = t0 + timedelta(minutes=index + 1)
        if not (
            frame.gap_free
            and frame.minute_complete
            and frame.bid is not None
            and frame.ask is not None
            and frame.bid <= frame.ask
            and frame.at <= frame.received_at <= frame.available_at <= min(end, now)
        ):
            continue  # Never substitute missing quotes or late arrivals.
        buckets.setdefault(index, []).append((frame.bid + frame.ask) / 2)
    pre = [Decimal(median(buckets[i])) for i in range(-15, -1) if i in buckets]
    if len(pre) < 10:
        reject("PRE_WINDOW_FEWER_THAN_10_MINUTES")
    missing_run = 0
    for i in range(-15, -1):
        missing_run = 0 if i in buckets else missing_run + 1
        if missing_run > 2:
            reject("PRE_WINDOW_GAP_OVER_TWO_MINUTES")
    if any(i not in buckets for i in (4, 9, 14)):
        reject("M5_M10_M15_WINDOW_MISSING")
    if now < t0 + timedelta(minutes=15):
        reject("EVALUATION_NOT_DUE")
    return (
        Decimal(median(pre)),
        Decimal(median(buckets[4])),
        Decimal(median(buckets[9])),
        Decimal(median(buckets[14])),
    )


def question(report: PollReport, candidate: Candidate, synthetic: bool) -> PollQuestion:
    hits = [
        q
        for q in report.questions
        if q.event.event_key(synthetic) == candidate.event.event_key(synthetic)
    ]
    if not hits:
        reject("REPORT_DOES_NOT_ASK_THIS_EVENT")
    if len(hits) != 1:
        reject("AMBIGUOUS_EVENT_QUESTIONS")
    return hits[0]


@dataclass(frozen=True)
class Signal:
    side: Literal["YES", "NO"]
    release_id: str
    old_release_id: str
    event_key: str
    sample_group: str
    z: Decimal
    contrast_change: Decimal
    medians: tuple[Decimal, Decimal, Decimal, Decimal]
    t0: datetime


def evaluate(candidate: Candidate, snapshot: Snapshot) -> Signal:
    now, synthetic = snapshot.now, snapshot.synthetic
    if not snapshot.catalog_complete or snapshot.catalog_markets <= 0:
        reject("CATALOG_PAGINATION_INCOMPLETE")
    expected_tick = Decimal("0.01") if synthetic else Decimal("0.005")
    if candidate.tick != expected_tick or candidate.lot != 1 or candidate.minimum_quantity % 1:
        reject("SIG_OR_SYNTHETIC_DOCUMENTED_UNITS_MISMATCH")
    if candidate.event.office == "CHAMBER_CONTROL":
        reject("CONTROL_MARKET_OBSERVE_ONLY")
    if (
        candidate.event.office not in {"HOUSE", "SENATE", "GOVERNOR"}
        or candidate.event.election_kind != "GENERAL"
    ):
        reject("MARKET_SCOPE_UNSUPPORTED")
    review = candidate.review
    kind = "SYNTHETIC" if synthetic else "OFFICIAL"
    if not (
        review.approved
        and review.reviewer
        and review.event_verified
        and review.binary_sides_verified
        and review.api_identity_verified
        and review.exchange_identity_verified
        and review.mapping_hash == candidate.mapping_hash
        and review.rules.kind == kind
        and review.rules.sha256 == candidate.rules_current_hash
        and review.approved_at is not None
        and review.expires_at is not None
        and review.rules.available_at <= review.approved_at <= now < review.expires_at
        and (candidate.page_id is None or review.page_identity_verified)
    ):
        reject("MARKET_RULES_OR_ID_MAPPING_UNREVIEWED")
    if not (
        review.fee_verified
        and review.fee_evidence
        and review.fee_evidence.kind == kind
        and review.fee_evidence.available_at <= now
        and review.buy_fee_bound is not None
        and review.sell_fee_bound is not None
    ):
        reject("SIG_FEE_BOUND_UNKNOWN")
    if not candidate.close_verified or candidate.earliest_event_or_close is None:
        reject("CLOSE_BOUNDARY_UNKNOWN")
    if candidate.earliest_event_or_close - now < timedelta(days=POLICY.terminal_buffer_days):
        reject("TERMINAL_BUFFER_UNDER_SEVEN_DAYS")
    if candidate.intervening_material_information:
        reject("INTERVENING_INFORMATION_OR_CORRECTION")
    reports = {r.release_id: r for r in snapshot.reports}
    if len(reports) != len(snapshot.reports):
        reject("DUPLICATE_RELEASE_IN_INPUT")
    if candidate.release_id not in reports:
        reject("ORIGINAL_REPORT_MISSING")
    new = reports[candidate.release_id]
    if new.withdrawn:
        reject("ORIGINAL_REPORT_WITHDRAWN")
    t0 = new.original.published_at
    if not new.original_verified or not new.first_public_verified or t0 is None:
        reject("FIRST_PUBLIC_OR_ORIGINAL_UNVERIFIED")
    if any(
        r.release_id != new.release_id
        and r.original.published_at
        and r.original.published_at > t0
        and r.original.available_at <= now
        and any(
            q.event.event_key(synthetic) == candidate.event.event_key(synthetic)
            for q in r.questions
        )
        for r in snapshot.reports
    ):
        reject("INTERVENING_EVENT_REPORT")
    source_kind = "SYNTHETIC" if synthetic else "ORIGINAL_PUBLIC"
    if new.original.kind != source_kind:
        reject("REPOST_IS_NOT_ORIGINAL")
    if not 0 <= (new.original.received_at - t0).total_seconds() <= 60:
        reject("REPORT_RECEIVED_OVER_60_SECONDS_LATE")
    if new.original.available_at > t0 + timedelta(minutes=15):
        reject("REPORT_NOT_AVAILABLE_BEFORE_EVALUATION")
    if now < t0 + timedelta(minutes=15):
        reject("EVALUATION_NOT_DUE")
    if now >= t0 + timedelta(minutes=16):
        reject("EVALUATION_OR_SEND_DEADLINE_PASSED")
    if not 0 <= (t0 - new.fieldwork_end).total_seconds() <= 72 * 3600:
        reject("FIELDWORK_PUBLICATION_OVER_72_HOURS")
    new_q = question(new, candidate, synthetic)
    if not new_q.mapping_verified:
        reject("POLL_QUESTION_EVENT_UNREVIEWED")
    series = new_q.series_key(new.organization, synthetic)
    comparable = []
    for report in snapshot.reports:
        if report.release_id == new.release_id or report.original.published_at is None:
            continue
        if report.original.published_at >= t0 or report.original.available_at > t0:
            continue
        for q in report.questions:
            if q.series_key(report.organization, synthetic) == series:
                comparable.append((report, q))
    if not comparable:
        reject("COMPARABLE_OLD_POLL_MISSING")
    old, old_q = max(comparable, key=lambda x: x[0].fieldwork_end)
    if not (
        old.original_verified
        and old.first_public_verified
        and old_q.mapping_verified
        and not old.withdrawn
        and old.original.kind == source_kind
    ):
        reject("OLD_POLL_SOURCE_OR_MAPPING_UNVERIFIED")
    old_mid = old.fieldwork_start + (old.fieldwork_end - old.fieldwork_start) / 2
    new_mid = new.fieldwork_start + (new.fieldwork_end - new.fieldwork_start) / 2
    if not timedelta(days=3) <= new_mid - old_mid <= timedelta(days=21):
        reject("POLL_MIDPOINT_SEPARATION_OUTSIDE_3_21_DAYS")
    if old.fieldwork_end >= new.fieldwork_start:
        reject("POLL_FIELDWORK_OVERLAP")
    if new_q.sample_group == "UNKNOWN" or old_q.sample_group == "UNKNOWN":
        reject("POLL_SAMPLE_ID_UNKNOWN")
    if new_q.sample_group == old_q.sample_group:
        reject("SHARED_SAMPLE_COVARIANCE_UNSUPPORTED")
    pair = next(
        (
            p
            for p in snapshot.comparisons
            if (p.old_release_id, p.new_release_id, p.event_key)
            == (old.release_id, new.release_id, candidate.event.event_key(synthetic))
        ),
        None,
    )
    if not (
        pair
        and pair.comparability_verified
        and pair.independence_verified
        and pair.stable_house_effect_assumed
        and pair.reviewer
        and pair.reviewed_at is not None
        and pair.evidence.kind == source_kind
        and pair.evidence.available_at <= pair.reviewed_at <= t0 + timedelta(minutes=15)
    ):
        reject("COMPARABILITY_OR_INDEPENDENCE_UNVERIFIED")
    assert pair is not None
    if (pair.old_report_digest, pair.new_report_digest) != (old.digest, new.digest):
        reject("REPORT_REVISION_NOT_REVIEWED")
    for q in (old_q, new_q):
        if q.contrast is None:
            reject("POLL_CONTRAST_MISSING")
        if (
            q.contrast_variance is None
            or q.variance_kind not in {"CONTRAST_DESIGN", "CONTRAST_MODEL"}
            or not q.variance_evidence
        ):
            reject("CONTRAST_VARIANCE_MISSING_OR_WRONG_TYPE")
    assert old_q.contrast is not None and new_q.contrast is not None
    assert old_q.contrast_variance is not None and new_q.contrast_variance is not None
    z = paired_poll_z(
        old_q.contrast,
        new_q.contrast,
        old_q.contrast_variance,
        new_q.contrast_variance,
        comparability_verified=True,
        independent_samples_verified=True,
    )
    change = new_q.contrast - old_q.contrast
    if abs(change) < Decimal("0.03") or abs(z) < 2:
        reject("POLL_CHANGE_OR_Z_BELOW_THRESHOLD")
    passing: list[tuple[Literal["YES", "NO"], tuple[Decimal, Decimal, Decimal, Decimal]]] = []
    sides: tuple[Literal["YES", "NO"], ...] = ("YES", "NO")
    for side in sides:
        if (
            side not in candidate.history
            or side not in candidate.books
            or side not in candidate.instrument_ids
        ):
            reject("BOTH_SIG_OUTCOME_BOOKS_REQUIRED")
        bid, ask = validate_book(candidate.books[side], now)
        values = window_medians(candidate.history[side], t0, now)
        if price_gate(*values, bid, ask):
            passing.append((side, values))
    if len(passing) > 1:
        reject("BOTH_OUTCOMES_PASS_PRICE_GATE")
    if not passing:
        reject("PRICE_CONFIRMATION_FAILED")
    side, values = passing[0]
    return Signal(
        side,
        new.release_id,
        old.release_id,
        candidate.event.event_key(synthetic),
        new_q.sample_group,
        z,
        change,
        values,
        t0,
    )


@dataclass(frozen=True)
class Plan:
    key: str
    event_key: str
    state: str
    release_id: str
    sample_group: str
    side: str
    instrument_id: str
    quantity: Decimal
    limit_price: Decimal
    entry_vwap: Decimal
    entry_cost_bound: Decimal
    immediate_exit_net: Decimal
    stop: Decimal
    gain: Decimal
    future_fee_per_share: Decimal
    total_reserved: Decimal
    z: Decimal
    contrast_change: Decimal
    score: Decimal
    created_at: datetime
    valid_until: datetime
    expected_pnl: None = None
    authorized_for_live: bool = False


def plan(candidate: Candidate, snapshot: Snapshot, signal: Signal | None = None) -> Plan:
    signal = signal or evaluate(candidate, snapshot)
    account = snapshot.account
    if not account.reconciled or account.unknown_requests:
        reject("ACCOUNT_UNRECONCILED_OR_UNKNOWN_REQUEST")
    if (
        1 - account.equity / account.high_water >= POLICY.peak_loss
        or 1 - account.equity / account.day_start_equity >= POLICY.daily_loss
    ):
        reject("EQUITY_SOFT_PAUSE")
    active = [e for e in account.exposures if e.active]
    occupied = {e.event_key for e in active}
    if signal.event_key in occupied:
        reject("DUPLICATE_UNDERLYING_EVENT")
    if len(occupied) >= POLICY.max_events:
        reject("FOURTH_CONCURRENT_EVENT")
    if any(e.state == candidate.event.state or e.state == "UNKNOWN" for e in active):
        reject("SAME_STATE_CONCENTRATION")
    if any(e.report_id == signal.release_id or e.report_id == "UNKNOWN" for e in active):
        reject("SAME_REPORT_CONCENTRATION")
    if any(e.sample_group == signal.sample_group or e.sample_group == "UNKNOWN" for e in active):
        reject("SHARED_SAMPLE_CONCENTRATION")
    exited = account.cooldowns.get(signal.event_key)
    if exited and (snapshot.now - exited).total_seconds() < POLICY.cooldown_seconds:
        reject("EVENT_24_HOUR_COOLDOWN")
    ref = min(account.initial, account.equity)
    open_risk = sum((e.amount for e in account.exposures), ZERO)
    budget = min(
        POLICY.single_fraction * ref,
        POLICY.total_fraction * ref - open_risk,
        POLICY.daily_fraction * account.initial - account.daily_committed,
        account.cash - account.reserved - POLICY.cash_floor * account.initial,
    )
    if budget <= 0:
        reject("ACCOUNT_TOTAL_DAILY_OR_CASH_BUDGET_EXHAUSTED")
    book = candidate.books[signal.side]
    bid, ask = validate_book(book, snapshot.now)
    fb, fs = candidate.review.buy_fee_bound, candidate.review.sell_fee_bound
    assert fb is not None and fs is not None
    entry_depth = sum((x.quantity for x in book.asks if x.price <= ask + Decimal("0.005")), ZERO)
    exit_depth = sum((x.quantity for x in book.bids if x.price >= bid - Decimal("0.005")), ZERO)
    qty = floor(
        min(budget / (ask + fb + fs), Decimal("0.10") * min(entry_depth, exit_depth)), candidate.lot
    )
    for _ in range(80):
        if qty < candidate.minimum_quantity:
            reject("CAPACITY_BELOW_MINIMUM_QUANTITY")
        avg, worst = consume(book.asks, qty, True)
        exit_avg, exit_worst = consume(book.bids, qty, False)
        if (
            worst > ask + Decimal("0.005")
            or worst > Decimal("0.80")
            or exit_worst < bid - Decimal("0.005")
        ):
            qty = floor(qty / 2, candidate.lot)
            continue
        if worst % candidate.tick:
            reject("SIG_PRICE_GRID_MISMATCH")
        entry = worst + fb
        net = exit_avg - fs
        stop = max(Decimal("0.02"), Decimal("1.5") * (entry - net))
        gain = 2 * stop
        if stop > Decimal("0.06") or entry + gain > 1 - fs:
            reject("ROUNDTRIP_COST_OR_NET_TARGET_INFEASIBLE")
        risk = qty * (entry + fs)
        if risk > budget or qty * stop > POLICY.stop_fraction * ref:
            qty = floor(
                min(qty - candidate.lot, budget / (entry + fs), POLICY.stop_fraction * ref / stop),
                candidate.lot,
            )
            continue
        if qty * worst < candidate.minimum_notional:
            reject("MINIMUM_NOTIONAL_NOT_MET")
        return Plan(
            candidate.key,
            signal.event_key,
            candidate.event.state,
            signal.release_id,
            signal.sample_group,
            signal.side,
            candidate.instrument_ids[signal.side],
            qty,
            worst,
            avg,
            entry,
            net,
            stop,
            gain,
            fs,
            risk,
            signal.z,
            signal.contrast_change,
            abs(signal.z) / (entry + fs),
            snapshot.now,
            min(
                snapshot.now + timedelta(seconds=POLICY.plan_seconds),
                signal.t0 + timedelta(minutes=16),
            ),
        )
    reject("FULL_DEPTH_CAPACITY_SEARCH_EXHAUSTED")
    raise AssertionError("unreachable")


def allocate(snapshot: Snapshot) -> dict:
    account = snapshot.account.model_copy(deep=True)
    rejected, eligible, accepted = {}, [], []
    for candidate in snapshot.candidates:
        try:
            signal = evaluate(candidate, snapshot)
            proposal = plan(candidate, snapshot, signal)
            eligible.append((proposal.score, candidate, signal))
        except RiskRejected as exc:
            rejected[candidate.key] = exc.code
    evaluated_keys = set()
    for _, candidate, signal in sorted(eligible, key=lambda item: (-item[0], item[1].key)):
        release_event = (signal.release_id, signal.event_key)
        if release_event in evaluated_keys:
            rejected[candidate.key] = "PRC_RELEASE_EVENT_ALREADY_EVALUATED"
            continue
        evaluated_keys.add(release_event)
        try:
            current = snapshot.model_copy(update={"account": account})
            proposal = plan(candidate, current, signal)
        except RiskRejected as exc:
            rejected[candidate.key] = exc.code
            continue
        account.reserved += proposal.total_reserved
        account.daily_committed += proposal.total_reserved
        account.exposures.append(
            Exposure(
                event_key=proposal.event_key,
                state=proposal.state,
                report_id=proposal.release_id,
                sample_group=proposal.sample_group,
                amount=proposal.total_reserved,
            )
        )
        accepted.append(asdict(proposal))
    return {
        "strategy_id": "prc_sig",
        "strategy_version": "1.0",
        "layer": "MOCK" if snapshot.synthetic else "LOCAL_DATA_PLANNING",
        "synthetic": snapshot.synthetic,
        "status": "PASS_COMPUTATION_ONLY",
        "plans": accepted,
        "rejected": rejected,
        "rejection_counts": dict(Counter(rejected.values())),
        "funnel": {
            "catalog_markets": snapshot.catalog_markets,
            "catalog_complete": snapshot.catalog_complete,
            "candidate_cards": len(snapshot.candidates),
            "candidate_underlying_events": len(
                {c.event.event_key(snapshot.synthetic) for c in snapshot.candidates}
            ),
            "original_reports": len({r.release_id for r in snapshot.reports}),
            "poll_and_price_eligible_cards": len(eligible),
            "allocated_events": len(accepted),
        },
        "total_reserved": sum((p["total_reserved"] for p in accepted), ZERO),
        "cash_after_all_reserved": account.cash - account.reserved,
        "daily_committed_after": account.daily_committed,
        "expected_pnl": None,
        "authorized_for_live": False,
        "real_orders": 0,
        "network_calls": 0,
    }
