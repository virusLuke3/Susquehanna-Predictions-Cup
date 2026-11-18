"""ECR-SIG v1: offline planning only. No network, credentials or order writes.
All inputs are normalized by a separately verified venue adapter.
Prices are price observations, not forecasts of electoral outcomes.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import ROUND_FLOOR, Decimal
from typing import Sequence

D = Decimal
ZERO = D("0")
ONE = D("1")


class Reject(ValueError):
    pass


def decimal(value: str | int | Decimal) -> Decimal:
    if isinstance(value, (float, bool)):
        raise Reject("use decimal strings, not float/bool")
    x = D(value)
    if not x.is_finite():
        raise Reject("non-finite value")
    return x


def floor_lot(value: Decimal, lot: Decimal) -> Decimal:
    if lot <= 0 or not value.is_finite() or not lot.is_finite():
        raise Reject("invalid lot/value")
    return max(ZERO, (value / lot).to_integral_value(rounding=ROUND_FLOOR) * lot)


@dataclass(frozen=True)
class Level:
    price: Decimal
    size: Decimal

    def __post_init__(self):
        if not self.price.is_finite() or not self.size.is_finite():
            raise Reject("non-finite book")
        if not (ZERO < self.price < ONE) or self.size <= 0:
            raise Reject("invalid level")


@dataclass(frozen=True)
class Book:
    bids: tuple[Level, ...]
    asks: tuple[Level, ...]
    verified_at: int
    gap_free: bool = True

    def check(self, now: int, max_age: int = 5) -> None:
        if not self.gap_free:
            raise Reject("book version gap")
        if not self.bids or not self.asks:
            raise Reject("missing two-sided book")
        if not 0 <= now - self.verified_at <= max_age:
            raise Reject("stale/future book")
        if self.bid >= self.ask:
            raise Reject("crossed/locked book: not this strategy")

    @property
    def bid(self):
        return max(x.price for x in self.bids)

    @property
    def ask(self):
        return min(x.price for x in self.asks)

    @property
    def mid(self):
        return (self.bid + self.ask) / 2


def consume(levels: Sequence[Level], qty: Decimal, buy: bool) -> tuple[Decimal, Decimal]:
    if qty <= 0:
        raise Reject("nonpositive quantity")
    left, total, marginal = qty, ZERO, ZERO
    for level in sorted(levels, key=lambda x: x.price, reverse=not buy):
        take = min(left, level.size)
        total += take * level.price
        marginal = level.price
        left -= take
        if not left:
            return total / qty, marginal
    raise Reject("insufficient full-size depth")


def depth(levels: Sequence[Level], low: Decimal, high: Decimal) -> Decimal:
    return sum((x.size for x in levels if low <= x.price <= high), ZERO)


@dataclass(frozen=True)
class Observation:
    at: int
    sig_bid: Decimal
    sig_ask: Decimal
    # One price tuple per genuinely distinct venue operator, same reviewed payoff.
    external: tuple[tuple[str, Decimal, Decimal], ...]
    gap_free: bool = True

    def validate(self):
        if not self.gap_free or not ZERO < self.sig_bid < self.sig_ask < ONE:
            raise Reject("bad historical quote")
        ids = [v for v, _, _ in self.external]
        if not ids or len(ids) != len(set(ids)):
            raise Reject("duplicate/missing source operator")
        for _, bid, ask in self.external:
            if not ZERO < bid < ask < ONE:
                raise Reject("bad external quote")


@dataclass(frozen=True)
class Candidate:
    key: str
    event_key: str
    state: str
    shock_group: str
    direction_bucket: str
    office: str
    side: str
    sig_book: Book
    external_books: tuple[tuple[str, Book], ...]
    baseline: Observation  # 30-35 min before latest observation
    history: tuple[Observation, ...]  # consecutive completed 1-min observations
    rules_reviewed: bool = False
    mapping_reviewed: bool = False
    rules_unchanged: bool = False
    fee_verified: bool = False
    buy_fee_bound: Decimal = ZERO
    sell_fee_bound: Decimal = ZERO
    lot: Decimal = ONE
    min_qty: Decimal = ONE
    price_tick: Decimal = ZERO
    catalyst_verified: bool = False
    catalyst_at: int | None = None
    catalyst_event_key: str | None = None
    last_event_exit_at: int | None = None
    tradable_until: int = 1793811600


@dataclass(frozen=True)
class Policy:
    dual_gap: Decimal = D("0.04")
    single_gap: Decimal = D("0.06")
    reference_haircut: Decimal = D("0.01")
    min_reference_move: Decimal = D("0.02")
    single_reference_move: Decimal = D("0.03")
    external_disagreement: Decimal = D("0.04")
    sig_spread: Decimal = D("0.02")
    external_spread: Decimal = D("0.03")
    ask_band: Decimal = D("0.005")
    bid_band: Decimal = D("0.02")
    price_low: Decimal = D("0.10")
    price_high: Decimal = D("0.90")
    child_fraction: Decimal = D("0.01")
    race_fraction: Decimal = D("0.03")
    total_fraction: Decimal = D("0.30")
    state_fraction: Decimal = D("0.08")
    shock_fraction: Decimal = D("0.08")
    direction_fraction: Decimal = D("0.15")
    nominal_stop_fraction: Decimal = D("0.001")
    cash_floor_fraction: Decimal = D("0.70")
    max_slots: int = 12
    max_holding_seconds: int = 21600
    daily_loss_limit: Decimal = D("0.03")
    peak_drawdown_pause: Decimal = D("0.08")
    last_entry_at: int = 1793707200  # 2026-11-03 12:00 UTC, design cutoff
    target_flat_at: int = 1793728800  # 2026-11-03 18:00 UTC, design cutoff


DEFAULT_POLICY = Policy()


@dataclass
class Account:
    initial: Decimal
    cash: Decimal
    equity: Decimal
    high_water: Decimal
    day_start_equity: Decimal
    reserved_and_future_fees: Decimal = ZERO
    total_exposure: Decimal = ZERO
    race_exposure: dict[str, Decimal] = field(default_factory=dict)
    state_exposure: dict[str, Decimal] = field(default_factory=dict)
    shock_exposure: dict[str, Decimal] = field(default_factory=dict)
    direction_exposure: dict[str, Decimal] = field(default_factory=dict)
    occupied_events: set[str] = field(default_factory=set)
    reconciled: bool = True
    unknown_writes: bool = False

    @property
    def wref(self):
        return min(self.initial, self.equity)

    def check(self, p: Policy):
        vals = (
            self.initial,
            self.cash,
            self.equity,
            self.high_water,
            self.day_start_equity,
            self.reserved_and_future_fees,
            self.total_exposure,
        )
        if any(not x.is_finite() or x < 0 for x in vals) or self.initial <= 0:
            raise Reject("bad account amounts")
        if not self.reconciled or self.unknown_writes:
            raise Reject("unreconciled account or unknown write")
        if self.high_water <= 0 or self.day_start_equity <= 0:
            raise Reject("bad equity reference")
        if 1 - self.equity / self.high_water >= p.peak_drawdown_pause:
            raise Reject("peak drawdown pause")
        if 1 - self.equity / self.day_start_equity >= p.daily_loss_limit:
            raise Reject("daily loss pause")
        if self.cash - self.reserved_and_future_fees < p.cash_floor_fraction * self.initial:
            raise Reject("cash floor already violated")
        for mapping in (
            self.race_exposure,
            self.state_exposure,
            self.shock_exposure,
            self.direction_exposure,
        ):
            if any(not amount.is_finite() or amount < ZERO for amount in mapping.values()):
                raise Reject("bad exposure amounts")


@dataclass(frozen=True)
class Signal:
    route: str
    min_gap: Decimal
    reference_probe: Decimal
    min_move: Decimal


@dataclass(frozen=True)
class Plan:
    key: str
    event_key: str
    side: str
    route: str
    quantity: Decimal
    limit_price: Decimal
    estimated_entry_vwap: Decimal
    entry_cost_bound_per_share: Decimal
    total_reserved: Decimal
    external_reference: Decimal
    gap_after_haircut: Decimal
    current_exit_net: Decimal
    target_gain_per_share: Decimal
    stop_trigger_per_share: Decimal
    score: Decimal
    state: str
    shock_group: str
    direction_bucket: str
    created_at: int
    valid_until: int
    # This is an estimated opportunity score, not expected P&L.
    expected_pnl: None = None
    authorized_for_live: bool = False


def check_candidate(c: Candidate, now: int, p: Policy = DEFAULT_POLICY) -> list[str]:
    if now >= p.last_entry_at:
        raise Reject("last entry cutoff")
    if not all((c.rules_reviewed, c.mapping_reviewed, c.rules_unchanged, c.fee_verified)):
        raise Reject("unverified rules/mapping/fees")
    if c.office not in {"HOUSE", "SENATE", "GOVERNOR"} or c.side not in {"YES", "NO"}:
        raise Reject("out of scope market")
    if not all((c.event_key, c.state, c.shock_group, c.direction_bucket)):
        raise Reject("missing exposure grouping")
    if c.lot <= 0 or c.min_qty <= 0 or c.price_tick <= 0:
        raise Reject("missing quantity/tick contract")
    if any(not x.is_finite() or x < 0 for x in (c.buy_fee_bound, c.sell_fee_bound)):
        raise Reject("bad fee bounds")
    if (
        now + p.max_holding_seconds + 3600 >= c.tradable_until
        or now + p.max_holding_seconds >= p.target_flat_at
    ):
        raise Reject("insufficient exit buffer")
    if c.last_event_exit_at is not None and now - c.last_event_exit_at < 21600:
        raise Reject("event cooldown")
    c.sig_book.check(now)
    venues = [v for v, _ in c.external_books]
    if len(set(venues)) != len(venues) or len(venues) not in {1, 2}:
        raise Reject("need one or two distinct operator sources")
    for _, b in c.external_books:
        b.check(now)
        if b.ask - b.bid > p.external_spread:
            raise Reject("wide external book")
    if c.sig_book.ask - c.sig_book.bid > p.sig_spread:
        raise Reject("wide SIG spread")
    if not p.price_low <= c.sig_book.ask <= p.price_high:
        raise Reject("price outside guard")
    return venues


def signal(c: Candidate, now: int, p: Policy = DEFAULT_POLICY) -> Signal:
    venues = check_candidate(c, now, p)
    if len(venues) == 1:
        if not c.catalyst_verified or c.catalyst_at is None:
            raise Reject("one-venue route needs original public catalyst")
        if not 0 <= now - c.catalyst_at <= 21600 or c.catalyst_event_key != c.event_key:
            raise Reject("wrong/stale catalyst")
        count, min_gap, move = 15, p.single_gap, p.single_reference_move
        route = "PUBLIC_EVENT_ONE_VENUE"
    else:
        count, min_gap, move = 5, p.dual_gap, p.min_reference_move
        route = "TWO_VENUE_REPRICING"
    if len(c.history) < count:
        raise Reject("insufficient short observation window")
    hist = c.history[-count:]
    if not 0 <= now - hist[-1].at <= 10:
        raise Reject("stale signal window")
    c.baseline.validate()
    if not 1800 <= hist[-1].at - c.baseline.at <= 2100:
        raise Reject("baseline must precede signal by 30-35 minutes")
    base = {v: (b, a) for v, b, a in c.baseline.external}
    if set(base) != set(venues):
        raise Reject("baseline source mismatch")
    for j, o in enumerate(hist):
        o.validate()
        if j and o.at - hist[j - 1].at != 60:
            raise Reject("history gap/duplicate time")
        if {v for v, _, _ in o.external} != set(venues):
            raise Reject("history source mismatch")
        if o.sig_ask - o.sig_bid > p.sig_spread:
            raise Reject("historical SIG spread")
        if any(a - b > p.external_spread for _, b, a in o.external):
            raise Reject("historical external spread")
        bids = [b for _, b, _ in o.external]
        if max(bids) - min(bids) > p.external_disagreement:
            raise Reject("external disagreement")
        if (
            min(bids) - o.sig_ask - c.buy_fee_bound - c.sell_fee_bound - p.reference_haircut
            < min_gap
        ):
            raise Reject("gap did not persist")
    last = hist[-1]
    moves = [b - base[v][0] for v, b, _ in last.external]
    if min(moves) < move:
        raise Reject("no corroborated external repricing")
    localmove = (last.sig_bid + last.sig_ask - c.baseline.sig_bid - c.baseline.sig_ask) / 2
    if localmove > min(moves) / 2:
        raise Reject("SIG already followed")
    current_bids = [b.bid for _, b in c.external_books]
    if max(current_bids) - min(current_bids) > p.external_disagreement:
        raise Reject("current external disagreement")
    if min(b.bid - base[v][0] for v, b in c.external_books) < move:
        raise Reject("external move reversed before planning")
    return Signal(route, min_gap, min(current_bids), min(moves))


def plan(
    c: Candidate, a: Account, now: int, p: Policy = DEFAULT_POLICY, *, signal_check=None
) -> Plan:
    a.check(p)
    s = (signal_check or signal)(c, now, p)
    if c.event_key in a.occupied_events:
        raise Reject("one live thesis per underlying event")
    if len(a.occupied_events) >= p.max_slots:
        raise Reject("position slots full")
    W = a.wref
    buydepth = depth(c.sig_book.asks, ZERO, c.sig_book.ask + p.ask_band)
    selldepth = depth(c.sig_book.bids, c.sig_book.bid - p.bid_band, ONE)
    extdepth = min(depth(b.bids, b.bid - p.bid_band, ONE) for _, b in c.external_books)
    budget = min(
        p.child_fraction * W,
        p.race_fraction * W - a.race_exposure.get(c.event_key, ZERO),
        p.total_fraction * W - a.total_exposure,
        p.state_fraction * W - a.state_exposure.get(c.state, ZERO),
        p.shock_fraction * W - a.shock_exposure.get(c.shock_group, ZERO),
        p.direction_fraction * W - a.direction_exposure.get(c.direction_bucket, ZERO),
        a.cash - a.reserved_and_future_fees - p.cash_floor_fraction * a.initial,
    )
    if budget <= 0:
        raise Reject("no risk budget")
    # Optimistic upper bound only; full-size prices and all budgets are checked below.
    denom = c.sig_book.ask + c.buy_fee_bound + c.sell_fee_bound
    qty = floor_lot(
        min(budget / denom, buydepth / D(10), selldepth / D(10), extdepth / D(5)), c.lot
    )
    for _ in range(40):
        if qty < c.min_qty:
            raise Reject("below minimum quantity after constraints")
        try:
            avg, marginal = consume(c.sig_book.asks, qty, True)
            if marginal > c.sig_book.ask + p.ask_band or marginal > p.price_high:
                raise Reject("entry slippage")
            # Fixed-share protected limit: actual cost cannot exceed this bound.
            limit = (marginal / c.price_tick).to_integral_value(rounding=ROUND_FLOOR) * c.price_tick
            if limit < marginal:
                raise Reject("off-grid book/contract mismatch")
            e = limit + c.buy_fee_bound
            current_exit = consume(c.sig_book.bids, qty, False)[0] - c.sell_fee_bound
            if consume(c.sig_book.bids, qty, False)[1] < c.sig_book.bid - p.bid_band:
                raise Reject("exit protection depth")
            ref = min(consume(b.bids, max(D(100), 5 * qty), False)[0] for _, b in c.external_books)
            gap = ref - e - c.sell_fee_bound - p.reference_haircut
            cost = qty * (e + c.sell_fee_bound)
            G = (ref - e - c.sell_fee_bound) / 2
            k = e - current_exit
            L = max(D("0.02"), 2 * k, G / 2)
            if k < 0:
                raise Reject("immediate cross not in strategy")
            if gap < s.min_gap or G <= 0 or L > D("0.06") or G < L:
                raise Reject("full-size opportunity insufficient")
            if e + G > ONE - c.sell_fee_bound:
                raise Reject("unreachable target")
            if cost > budget or qty * L > p.nominal_stop_fraction * W:
                # This step may be conservative, not a proof of maximal capacity.
                bounds = [qty - c.lot]
                if cost > budget:
                    bounds.append(budget / (e + c.sell_fee_bound))
                if qty * L > p.nominal_stop_fraction * W:
                    bounds.append(p.nominal_stop_fraction * W / L)
                qty = floor_lot(min(bounds), c.lot)
                continue
            return Plan(
                c.key,
                c.event_key,
                c.side,
                s.route,
                qty,
                limit,
                avg,
                e,
                cost,
                ref,
                gap,
                current_exit,
                G,
                L,
                gap / (e + c.sell_fee_bound),
                c.state,
                c.shock_group,
                c.direction_bucket,
                now,
                now + 5,
            )
        except Reject:
            qty = floor_lot(qty / 2, c.lot)
    raise Reject("quantity search exhausted")


def reserve(a: Account, proposal: Plan, now: int, p: Policy = DEFAULT_POLICY) -> None:
    """Offline reservation simulation. A production implementation MUST use a DB transaction."""
    a.check(p)
    if now >= proposal.valid_until or now < proposal.created_at:
        raise Reject("expired/future proposal")
    if (
        proposal.quantity <= ZERO
        or proposal.total_reserved <= ZERO
        or proposal.total_reserved > p.child_fraction * a.wref
    ):
        raise Reject("invalid child reservation")
    if proposal.event_key in a.occupied_events:
        raise Reject("duplicate event proposal")
    W = a.wref
    limits = [
        (a.total_exposure, p.total_fraction),
        (a.race_exposure.get(proposal.event_key, ZERO), p.race_fraction),
        (a.state_exposure.get(proposal.state, ZERO), p.state_fraction),
        (a.shock_exposure.get(proposal.shock_group, ZERO), p.shock_fraction),
        (a.direction_exposure.get(proposal.direction_bucket, ZERO), p.direction_fraction),
    ]
    if any(old + proposal.total_reserved > frac * W for old, frac in limits):
        raise Reject("reservation exceeds updated exposure budget")
    if len(a.occupied_events) >= p.max_slots:
        raise Reject("updated slot limit")
    if (
        a.cash - a.reserved_and_future_fees - proposal.total_reserved
        < p.cash_floor_fraction * a.initial
    ):
        raise Reject("reservation violates cash floor")
    a.reserved_and_future_fees += proposal.total_reserved
    a.total_exposure += proposal.total_reserved
    for mapping, key in (
        (a.race_exposure, proposal.event_key),
        (a.state_exposure, proposal.state),
        (a.shock_exposure, proposal.shock_group),
        (a.direction_exposure, proposal.direction_bucket),
    ):
        mapping[key] = mapping.get(key, ZERO) + proposal.total_reserved
    a.occupied_events.add(proposal.event_key)


def allocate(candidates: Sequence[Candidate], a: Account, now: int, p: Policy = DEFAULT_POLICY):
    if len({c.key for c in candidates}) != len(candidates):
        raise Reject("duplicate candidate key")
    pre, rejected = [], {}
    for c in candidates:
        try:
            pre.append((plan(c, a, now, p).score, c))
        except Reject as e:
            rejected[c.key] = str(e)
    accepted = []
    for _, c in sorted(pre, key=lambda x: (-x[0], x[1].key)):
        try:
            proposal = plan(c, a, now, p)
            reserve(a, proposal, now, p)
            accepted.append(proposal)
        except Reject as e:
            rejected[c.key] = str(e)
    return accepted, rejected


def exit_reason(
    proposal: Plan,
    net_exit: Decimal | None,
    current_reference: Decimal | None,
    now: int,
    force_exit: bool = False,
    rules_changed: bool = False,
    reference_missing_seconds: int = 0,
    p: Policy = DEFAULT_POLICY,
    holding_started_at: int | None = None,
) -> str | None:
    if holding_started_at is None:
        return "NO_FILLED_HOLDING"
    if holding_started_at > now:
        raise Reject("future holding start")
    force_exit = force_exit or now >= p.target_flat_at
    if rules_changed or force_exit:
        return "EXIT_BLOCKED" if net_exit is None else "CONTROLLED_EXIT"
    if net_exit is None:
        return "EXIT_BLOCKED"
    gain = net_exit - proposal.entry_cost_bound_per_share
    if gain >= proposal.target_gain_per_share:
        return "TAKE_PROFIT"
    if gain <= -proposal.stop_trigger_per_share:
        return "STOP_TRIGGER"
    if (
        current_reference is not None
        and current_reference
        <= proposal.entry_cost_bound_per_share + proposal.target_gain_per_share / 2
    ):
        return "REFERENCE_REVERSED"
    if reference_missing_seconds >= 120:
        return "REFERENCE_LOST"
    if now - holding_started_at >= p.max_holding_seconds:
        return "TIME_EXIT"
    return None


def expected_pnl_sensitivity(
    qty: Decimal,
    G: Decimal,
    L: Decimal,
    cost: Decimal,
    success_given_no_tail: Decimal,
    tail: Decimal,
) -> Decimal:
    """Synthetic scenario calculation, NOT a fitted expectation."""
    q, eps = success_given_no_tail, tail
    if not ZERO <= q <= ONE or not ZERO <= eps < ONE:
        raise Reject("invalid scenario weight")
    return qty * ((1 - eps) * (q * G - (1 - q) * L) - eps * cost)
