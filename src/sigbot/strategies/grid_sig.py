"""Single-exchange geometric grid, using the common OMS and actual FIFO lots.

Historical trade VWAP estimates a range; only official fills create inventory.
No paired-market prices, synthetic books, netting, or simulated executions.
"""

import json
from datetime import timedelta
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from statistics import median

from pydantic import Field
from sqlalchemy import select

from sigbot.domain import TERMINAL, ZERO, Intent, Money, StrictModel, utc_time
from sigbot.errors import RiskRejected
from sigbot.risk import Planner
from sigbot.storage import (
    FillRow,
    Instrument,
    IntentRow,
    Order,
    Position,
    PositionLot,
    StrategyEvent,
)

IDENTITY = ("grid_sig", "1.0")
D = Decimal


class Parameters(StrictModel):
    window_hours: int = Field(default=6, ge=6, le=6, strict=True)
    min_observations: int = Field(default=24, ge=24, le=24, strict=True)
    history_max_age_seconds: int = Field(default=1800, ge=1, le=1800, strict=True)
    order_cash: Money = Field(default="10", gt=0, le=10, validate_default=True)
    market_cash: Money = Field(default="30", gt=0, le=30, validate_default=True)
    strategy_cash: Money = Field(default="1000", gt=0, le=50000, validate_default=True)
    width_floor: Money = Field(default="0.005", gt=0, validate_default=True)
    stop_distance: Money = Field(default="0.015", gt=0, validate_default=True)
    holding_seconds: int = Field(default=10800, ge=1, le=10800, strict=True)
    order_seconds: int = Field(default=300, ge=30, le=300, strict=True)


def floor_tick(price, tick):
    return (price / tick).to_integral_value(rounding=ROUND_FLOOR) * tick


def ceil_tick(price, tick):
    return (price / tick).to_integral_value(rounding=ROUND_CEILING) * tick


def profile(rows, at, parameters=None):
    """Calendar [t-W,t), closed and already available buckets only; never fill NA."""
    p = parameters or Parameters()
    at = utc_time(at)
    asof = at.replace(minute=at.minute // 5 * 5, second=0, microsecond=0)
    left = asof - timedelta(hours=p.window_hours)
    local = []
    seen = set()
    for row in rows:
        end = utc_time(row["end"])
        if left <= end < asof and utc_time(row["available_at"]) <= at:
            if end in seen:
                raise RiskRejected("GRID_DUPLICATE_BUCKET")
            seen.add(end)
            value = D(row["mean"])
            if not value.is_finite() or not 0 < value < 1:
                raise RiskRejected("GRID_INVALID_HISTORY_PRICE")
            local.append((end, value))
    local.sort()
    result = {"asof": asof.isoformat(), "count": len(local), "eligible": False}
    if not local:
        return {**result, "reason": "NO_CLOSED_HISTORY"}
    latest, last = local[-1]
    mu = median([v for _, v in local])
    width = max(p.width_floor, D("1.4826") * median([abs(v - mu) for _, v in local]))
    midpoint = asof - timedelta(hours=p.window_hours / 2)
    early = [v for end, v in local if end < midpoint]
    late = [v for end, v in local if end >= midpoint]
    drift = median(late) - median(early) if min(len(early), len(late)) >= 3 else None
    reason = (
        "INSUFFICIENT_OBSERVATIONS"
        if len(local) < p.min_observations
        else "HISTORY_STALE"
        if (at - latest).total_seconds() > p.history_max_age_seconds
        else None
    )
    return {
        **result,
        "mu": str(mu),
        "width": str(width),
        "drift": str(drift) if drift is not None else None,
        "last": str(last),
        "latest_end": latest.isoformat(),
        "eligible": reason is None,
        "reason": reason,
        "data_grade": "E0_TRADE_VWAP_SIGNAL_ONLY",
    }


def envelope(intent, identity=IDENTITY):
    if (intent.strategy_id, intent.strategy_version) != identity:
        raise RiskRejected("GRID_IDENTITY_MISMATCH")
    note = json.loads(intent.note or "{}")
    if note.get("kind") not in {"ENTRY", "EXIT", "RISK_EXIT"}:
        raise RiskRejected("GRID_METADATA_REQUIRED")
    return note


def own_orders(session, account, *, active=False, identity=IDENTITY):
    result = []
    for order, origin in session.execute(
        select(Order, IntentRow)
        .join(IntentRow, Order.intent_id == IntentRow.id)
        .where(
            Order.account_id == account,
            IntentRow.payload["strategy_id"].as_string() == identity[0],
            IntentRow.payload["strategy_version"].as_string() == identity[1],
        )
    ):
        if (origin.payload.get("strategy_id"), origin.payload.get("strategy_version")) != identity:
            continue
        if not active or (
            order.state not in TERMINAL
            or order.request_state != "RESOLVED"
            or order.reserve_cash
            or order.reserve_quantity
        ):
            result.append((order, origin))
    return result


def owned_lots(session, account, instrument=None, *, identity=IDENTITY):
    result = []
    query = select(PositionLot).where(
        PositionLot.account_id == account, PositionLot.quantity > ZERO
    )
    if instrument:
        query = query.where(PositionLot.instrument_id == instrument)
    for lot in session.scalars(query.order_by(PositionLot.id)):
        fill = session.get(FillRow, (account, lot.economic_id.removeprefix("fill:")))
        order = session.get(Order, fill.order_id) if fill else None
        origin = session.get(IntentRow, order.intent_id) if order else None
        if (
            origin
            and (origin.payload.get("strategy_id"), origin.payload.get("strategy_version"))
            == identity
        ):
            result.append((lot, envelope(Intent.model_validate(origin.payload), identity)))
    return result


def finances(session, account, parameters):
    """Cash is reconstructed from real common-ledger fills, never mark-to-market."""
    from sigbot.accounting import strategy_finances

    funds = strategy_finances(session, account, IDENTITY[0], parameters.strategy_cash)
    return {
        "cash": funds["cash"],
        "reserved": funds["reserved_cash"],
        "cost": funds["cost"],
        "available": funds["available"],
        "realized_pnl": funds["realized_pnl"],
    }


class GridPlanner(Planner):
    identity = IDENTITY
    parameter_class = Parameters
    risk_reasons = {"STOP", "TIMEOUT", "HISTORY_GAP", "REGIME_BREAK"}

    def envelope(self, intent):
        return envelope(intent, self.identity)

    def own_orders(self, session, account, **kwargs):
        return own_orders(session, account, identity=self.identity, **kwargs)

    def owned_lots(self, session, account, instrument=None):
        return owned_lots(session, account, instrument, identity=self.identity)

    def rebuild_intent(self, session, account, intent):
        """Apply current executable-side guards without moving the frozen target."""
        from sigbot.strategies.mm_sig_v1.core import external_best
        from sigbot.strategies.mm_sig_v1.models import Book

        n = self.envelope(intent)
        inst = session.get(Instrument, intent.instrument_id)
        book = Book.model_validate(
            {**inst.book, "received_at": inst.book_at, "quality": inst.quality}
        )
        bid, ask = external_best(book)
        if n["kind"] == "ENTRY":
            price = min(intent.limit_price, floor_tick(ask - inst.tick, inst.tick))
            if price <= 0:
                raise RiskRejected("GRID_PASSIVE_PRICE_BELOW_TICK")
            return intent.model_copy(update={"limit_price": price})
        if n["kind"] == "RISK_EXIT":
            pos = session.get(Position, (account.id, inst.id))
            held = pos.quantity if pos else ZERO
            depth = sum((x.quantity - x.own_quantity for x in book.bids if x.price == bid), ZERO)
            quantity = min(held - intent.target_quantity, depth).to_integral_value(
                rounding=ROUND_FLOOR
            )
            return intent.model_copy(
                update={"limit_price": bid, "target_quantity": held - max(ZERO, quantity)}
            )
        return intent

    def market_data_time(self, inst, intent):
        stamp = inst.book.get("validated_at")
        if stamp is None:
            raise RiskRejected("GRID_BOOK_VALIDATION_REQUIRED")
        return utc_time(stamp)

    def can_coexist(self, intent, existing):
        return (existing.get("strategy_id"), existing.get("strategy_version")) == self.identity

    def check_reduce_pending_buy(self, intent, pending_buy):
        self.envelope(intent)  # Ordinary covered exits may coexist with a deeper resting buy.

    def event(self, session, account, exchange):
        return session.get(StrategyEvent, (account, *self.identity, exchange))

    def validate_signal(self, session, account, intent, price, quantity, *, dispatch=False):
        n = self.envelope(intent)
        inst = session.get(Instrument, intent.instrument_id)
        self.parameter_class.model_validate(n["parameters"])
        if n.get("exchange_id") != inst.exchange_id:
            raise RiskRejected("GRID_EXCHANGE_BINDING_CHANGED")
        event = self.event(session, account.id, inst.exchange_id)
        active = self.own_orders(session, account.id, active=True)
        position = session.get(Position, (account.id, inst.id))
        lots = self.owned_lots(session, account.id, inst.id)
        if position and position.quantity != sum((lot.quantity for lot, _ in lots), ZERO):
            raise RiskRejected("GRID_FOREIGN_FIFO_INVENTORY_PROTECTED")
        if event and event.state != "CLOSED" and event.instrument_id != inst.id:
            raise RiskRejected("GRID_OPPOSITE_DIRECTION_OCCUPIED")
        # Buying the other outcome can burn another strategy's shares. Protect
        # both directions before admission, not only at wire normalization.
        for other in session.scalars(
            select(Instrument).where(
                Instrument.tournament == inst.tournament,
                Instrument.exchange_id == inst.exchange_id,
            )
        ):
            if other.id == inst.id:
                continue
            pos = session.get(Position, (account.id, other.id))
            pending = session.scalar(
                select(Order.id).where(
                    Order.account_id == account.id,
                    Order.instrument_id == other.id,
                    (Order.reserve_cash > ZERO) | (Order.reserve_quantity > ZERO),
                )
            )
            if pos and pos.quantity or pending:
                raise RiskRejected("GRID_OPPOSITE_REAL_ASSET_PROTECTED")
        if n["kind"] != "ENTRY":
            lots = self.owned_lots(session, account.id, inst.id)
            if not lots or not event or event.instrument_id != inst.id:
                raise RiskRejected("GRID_COVERED_OWN_EXIT_REQUIRED")
            available = sum((lot.quantity for lot, _ in lots), ZERO)
            reserved = sum(
                (o.reserve_quantity for o, _ in active if o.instrument_id == inst.id), ZERO
            )
            if intent.action != "REDUCE_TO_TARGET" or quantity > available - (
                ZERO if dispatch else reserved
            ):
                raise RiskRejected("GRID_NO_OVERSELL")
            if n["kind"] == "RISK_EXIT" and (
                n.get("reason") not in self.risk_reasons
                or event.data.get("risk_exit") != n.get("reason")
            ):
                raise RiskRejected("GRID_PERSISTED_RISK_EXIT_REASON_REQUIRED")
            if n["kind"] == "EXIT":
                first, frozen = lots[0]
                target = max(
                    D(frozen["target"]),
                    ceil_tick(first.cost / first.quantity + inst.tick, inst.tick),
                )
                if price < target or quantity > first.quantity:
                    raise RiskRejected("GRID_FROZEN_FIFO_TARGET_REQUIRED")
            return
        self.validate_entry(session, account, intent, price, quantity, dispatch=dispatch)

    def validate_entry(self, session, account, intent, price, quantity, *, dispatch=False):
        n = self.envelope(intent)
        inst = session.get(Instrument, intent.instrument_id)
        p = self.parameter_class.model_validate(n["parameters"])
        event = self.event(session, account.id, inst.exchange_id)
        active = self.own_orders(session, account.id, active=True)
        if intent.action != "INCREASE_TO_TARGET" or not 1 <= n.get("slot", 0) <= 3:
            raise RiskRejected("GRID_ENTRY_SLOT_REQUIRED")
        if event and event.data.get("risk_exit"):
            raise RiskRejected("GRID_RISK_EXIT_LATCHED")
        f = n["profile"]
        if (
            not f.get("eligible")
            or not 0
            <= (self.clock.now - utc_time(f["latest_end"])).total_seconds()
            <= p.history_max_age_seconds
        ):
            raise RiskRejected("GRID_HISTORY_STALE_OR_INELIGIBLE")
        if not 0 <= (self.clock.now - utc_time(f["asof"])).total_seconds() <= 600:
            raise RiskRejected("GRID_PROFILE_EXPIRED")
        drift = D(f["drift"]) if f.get("drift") is not None else ZERO
        if (drift if inst.outcome == "YES" else -drift) <= D("-0.01"):
            raise RiskRejected("GRID_PERSISTENT_ADVERSE_DRIFT")
        if price > D(n["level"]) or D(n["target"]) - price < inst.tick:
            raise RiskRejected("GRID_ENTRY_EDGE_TOO_SMALL")
        lots = self.owned_lots(session, account.id, inst.id)
        occupied = {meta["slot"] for _, meta in lots}
        occupied.update(
            json.loads(origin.payload["note"])["slot"]
            for o, origin in active
            if o.instrument_id == inst.id and o.side == "BUY" and o.intent_id != intent.intent_id
        )
        if n["slot"] in occupied:
            raise RiskRejected("GRID_SLOT_ALREADY_OCCUPIED")
        cost = sum((lot.cost for lot, _ in lots), ZERO)
        reserved = sum((o.reserve_cash for o, _ in active if o.instrument_id == inst.id), ZERO)
        own_reserve = quantity * price if dispatch else ZERO
        extra = ZERO if dispatch else quantity * price
        funds = finances(session, account.id, p)
        if (
            quantity * price > p.order_cash
            or cost + reserved + extra > p.market_cash
            or quantity * price > funds["available"] + own_reserve
        ):
            raise RiskRejected("GRID_CASH_BUDGET_EXCEEDED")

    def calculate(self, session, account, intent):
        plan = super().calculate(session, account, intent)
        self.validate_signal(session, account, intent, plan.price, plan.quantity)
        return plan

    def check_liquidity(self, inst, side, price, quantity, intent):
        from sigbot.strategies.mm_sig_v1.core import external_best
        from sigbot.strategies.mm_sig_v1.models import Book

        book = Book.model_validate(
            {**inst.book, "received_at": inst.book_at, "quality": inst.quality}
        )
        bid, ask = external_best(book)
        n = self.envelope(intent)
        if not book.own_orders_known or bid >= ask:
            raise RiskRejected("GRID_REAL_UNCROSSED_BOOK_REQUIRED")
        if side == "BUY" and price >= ask:
            raise RiskRejected("GRID_PASSIVE_BUY_WOULD_CROSS")
        if side == "SELL" and price <= bid:
            # A normal target may take a favorable actual bid; risk exits use
            # the same real depth check, with no fabricated bid/ask spread.
            depth = sum(
                (
                    level.quantity - level.own_quantity
                    for level in book.bids
                    if level.price >= price
                ),
                ZERO,
            )
            if n["kind"] == "RISK_EXIT" and quantity > depth:
                raise RiskRejected("GRID_REAL_EXIT_DEPTH_INSUFFICIENT")
            return
        if n["kind"] == "RISK_EXIT":
            raise RiskRejected("GRID_RISK_EXIT_NEEDS_CURRENT_BID")

    def check_dispatch(self, session, account, intent, order):
        self.validate_signal(session, account, intent, order.price, order.quantity, dispatch=True)

    def on_reserved(self, session, account, intent, plan, order_id):
        n = self.envelope(intent)
        inst = session.get(Instrument, intent.instrument_id)
        event = self.event(session, account.id, inst.exchange_id)
        if event is None:
            event = StrategyEvent(
                account_id=account.id,
                strategy_id=self.identity[0],
                strategy_version=self.identity[1],
                event_key=inst.exchange_id,
                instrument_id=inst.id,
                state="RESERVED",
                state_group="SINGLE_MARKET",
                shock_group="GEOMETRIC_GRID",
                direction_bucket=inst.outcome,
                future_fee_reserve=ZERO,
                data={},
            )
            session.add(event)
        if event.state == "CLOSED" or not event.data:
            event.instrument_id, event.direction_bucket = inst.id, inst.outcome
            event.holding_started_at = None
            event.data = {"mu": n["mu"], "width": n["width"], "cycle": order_id}
        event.state = "ENTRY_PENDING" if n["kind"] == "ENTRY" else "EXIT_PENDING"

    def update_event(self, session, account, order):
        inst = session.get(Instrument, order.instrument_id)
        event = self.event(session, account.id, inst.exchange_id)
        if not event:
            return
        lots = self.owned_lots(session, account.id, inst.id)
        active = [
            o
            for o, _ in self.own_orders(session, account.id, active=True)
            if o.instrument_id == inst.id
        ]
        if lots:
            event.state = "HOLDING"
            if event.holding_started_at is None:
                times = []
                for lot, _ in lots:
                    fill = session.get(FillRow, (account.id, lot.economic_id.removeprefix("fill:")))
                    stamp = fill.payload.get("filled_at") if fill else None
                    if stamp and stamp != "None":
                        times.append(utc_time(stamp))
                event.holding_started_at = min(times) if times else self.clock.now
        elif not active:
            event.state, event.last_exit_at = "CLOSED", self.clock.now
        else:
            event.state = "ENTRY_PENDING"

    def on_observed(self, session, account, order, observation):
        self.update_event(session, account, order)

    def on_unsent_expired(self, session, account, intent, order):
        self.update_event(session, account, order)

    def on_unsent_canceled(self, session, account, intent, order):
        self.update_event(session, account, order)
