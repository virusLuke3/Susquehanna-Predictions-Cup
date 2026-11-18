"""Decimal quote calculation. Public trades establish activity, never our fill rate."""

from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal

from sigbot.domain import ZERO
from sigbot.errors import RiskRejected
from sigbot.strategies.mm_sig_v1.models import Book, Candidate, Inventory, MmConfig, Snapshot


def external_best(book: Book) -> tuple[Decimal, Decimal]:
    bids = [x.price for x in book.bids if x.quantity > x.own_quantity]
    asks = [x.price for x in book.asks if x.quantity > x.own_quantity]
    if not book.own_orders_known:
        raise RiskRejected("MM_OWN_ORDERS_UNKNOWN")
    if not bids or not asks:
        raise RiskRejected("MM_TWO_SIDED_EXTERNAL_BOOK_REQUIRED")
    bid, ask = max(bids), min(asks)
    if bid >= ask:
        raise RiskRejected("MM_CROSSED_EXTERNAL_BOOK")
    return bid, ask


def check_book(book: Book, now: datetime, cfg: MmConfig) -> tuple[Decimal, Decimal]:
    if book.quality != "HEALTHY" or not (
        book.source_at <= book.received_at <= now
        and (now - book.source_at).total_seconds() <= cfg.book_seconds
        and (now - book.received_at).total_seconds() <= cfg.book_seconds
    ):
        raise RiskRejected("MM_BOOK_GAP_OR_STALE")
    return external_best(book)


@dataclass(frozen=True)
class Quote:
    instrument_id: str
    event_key: str
    side: str
    quantity: Decimal
    price: Decimal
    reference_bid: Decimal
    reference_ask: Decimal
    net_spread_buffer: Decimal
    inventory_only: bool


def quote_prices(book: Book, inventory: Decimal, tick: Decimal, now: datetime, cfg: MmConfig):
    """Same price calculation for actual books and deterministic OMS tests."""
    bid, ask = check_book(book, now, cfg)
    if tick <= 0 or inventory < 0:
        raise RiskRejected("MM_INVALID_PRICE_INPUT")
    skew = tick * cfg.inventory_skew_ticks * min(inventory / cfg.max_inventory, Decimal(1))
    buy = ((bid + cfg.improvement_ticks * tick - skew) / tick).to_integral_value(
        rounding=ROUND_FLOOR
    ) * tick
    sell = ((ask - cfg.improvement_ticks * tick - skew) / tick).to_integral_value(
        rounding=ROUND_CEILING
    ) * tick
    sell = max(sell, bid + tick)
    buy = min(buy, ask - tick)
    if not ZERO < buy < sell < 1:
        raise RiskRejected("MM_INVALID_QUOTE_PRICES")
    return buy, sell, bid, ask


def quote(c: Candidate, inv: Inventory, now: datetime, cfg: MmConfig) -> list[Quote]:
    if not c.reviewed or c.review_until <= now:
        raise RiskRejected("MM_RULES_MAPPING_REVIEW_REQUIRED")
    if c.status != "OPEN":
        raise RiskRejected("MM_MARKET_CLOSED")
    if c.fee_per_share is None:
        raise RiskRejected("MM_FEE_UNVERIFIED")
    if (
        not c.activity_window_complete
        or c.recent_trade_count is None
        or not (0 <= (now - c.activity_until).total_seconds() <= 60)
    ):
        raise RiskRejected("MM_ACTIVITY_WINDOW_INCOMPLETE")
    if c.recent_trade_count < cfg.min_recent_trades:
        raise RiskRejected("MM_NO_MEASURED_TRADING_DEMAND")
    if inv.quantity and inv.first_fill_at is None:
        raise RiskRejected("MM_INVENTORY_AGE_UNKNOWN")
    if inv.first_fill_at is not None and inv.first_fill_at > now:
        raise RiskRejected("MM_INVENTORY_CLOCK_INVALID")
    old = bool(
        inv.quantity
        and inv.first_fill_at is not None
        and (now - inv.first_fill_at).total_seconds() >= cfg.max_inventory_seconds
    )
    buy, sell, bid, ask = quote_prices(c.book, inv.quantity, c.tick, now, cfg)
    buffer = sell - buy - 2 * c.fee_per_share - cfg.exit_cost_buffer
    if not ZERO < buy < sell < 1 or buffer < cfg.min_net_spread:
        raise RiskRejected("MM_SPREAD_DOES_NOT_COVER_COST_BUFFER")
    result = []
    for side, size, price in (
        ("BUY", min(cfg.quote_quantity, max(ZERO, cfg.max_inventory - inv.quantity)), buy),
        ("SELL", min(cfg.quote_quantity, inv.quantity), sell),
    ):
        size = (size / c.step).to_integral_value(rounding=ROUND_FLOOR) * c.step
        if size and not (side == "BUY" and old):
            result.append(
                Quote(c.instrument_id, c.event_key, side, size, price, bid, ask, buffer, old)
            )
    return result


def plan(snapshot: Snapshot, cfg: MmConfig) -> dict:
    if snapshot.account.initial != cfg.initial_capital:
        raise ValueError("MM initial capital mismatch")
    plans: list[dict] = []
    rejected: list[dict] = []
    if snapshot.account.unknown_orders or not snapshot.account.reconciled:
        return {"status": "BLOCKED", "plans": [], "reason": "MM_ACCOUNT_NOT_READY"}
    if not snapshot.starts_at <= snapshot.now < snapshot.ends_at:
        return {"status": "BLOCKED", "plans": [], "reason": "MM_TRADING_CLOSED"}
    committed = snapshot.account.total_cost + snapshot.account.reserved
    cash = snapshot.account.cash - snapshot.account.reserved
    occupied = {
        c.event_key
        for c in snapshot.candidates
        if c.instrument_id in snapshot.account.inventory
        and snapshot.account.inventory[c.instrument_id].quantity
    }
    for c in snapshot.candidates:
        try:
            inv = snapshot.account.inventory.get(
                c.instrument_id, Inventory(quantity=ZERO, cost=ZERO)
            )
            if c.event_key in {x["event_key"] for x in plans}:
                raise RiskRejected("MM_ONE_CARD_PER_EVENT")
            proposals = quote(c, inv, snapshot.now, cfg)
            assert c.fee_per_share is not None
            local_cash, local_committed = cash, committed
            local_occupied = set(occupied)
            local_plans = []
            for q in proposals:
                cost = q.quantity * (q.price + c.fee_per_share)
                if cost > cfg.max_order_cost:
                    raise RiskRejected("MM_ORDER_COST_LIMIT")
                if q.side == "BUY":
                    bound = cost + q.quantity * c.fee_per_share
                    if (
                        bound > local_cash
                        or local_committed + bound > cfg.max_total_cost
                        or inv.cost + bound > cfg.max_event_cost
                    ):
                        raise RiskRejected("MM_COST_LIMIT")
                    if len(local_occupied | {c.event_key}) > cfg.max_events:
                        raise RiskRejected("MM_EVENT_LIMIT")
                    local_cash -= bound
                    local_committed += bound
                    local_occupied.add(c.event_key)
                local_plans.append(asdict(q))
            plans.extend(local_plans)
            cash, committed, occupied = local_cash, local_committed, local_occupied
        except RiskRejected as exc:
            rejected.append({"instrument_id": c.instrument_id, "reason": exc.code})
    return {
        "status": "PASS_COMPUTATION_ONLY",
        "strategy_id": cfg.strategy_id,
        "strategy_version": cfg.strategy_version,
        "plans": plans,
        "rejected": rejected,
        "real_orders": 0,
        "fill_model": "NO_FILLS_FROM_BOOK_TOUCH",
        "profit_verified": False,
    }
