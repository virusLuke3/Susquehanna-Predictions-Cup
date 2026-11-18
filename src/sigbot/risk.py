from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session

from sigbot.config import Config
from sigbot.domain import ZERO, Clock, Intent, Mode, check_grid, utc_time
from sigbot.errors import Conflict, RiskRejected
from sigbot.marketdata import executable_depth
from sigbot.storage import (
    Account,
    Instrument,
    IntentRow,
    Order,
    Outbox,
    Position,
    Store,
    StrategyEvent,
)


@dataclass(frozen=True)
class Plan:
    intent_id: str
    quantity: Decimal
    side: str
    price: Decimal
    reserve_cash: Decimal
    reserve_quantity: Decimal
    fee_bound: Decimal
    snapshot_at: datetime
    rules_hash: str
    mapping_hash: str
    risk_decision: dict | None = None


class Planner:
    def __init__(
        self, store: Store, config: Config, clock: Clock, starts_at: datetime, ends_at: datetime
    ):
        self.store, self.config, self.clock = store, config, clock
        self.starts_at, self.ends_at = utc_time(starts_at), utc_time(ends_at)

    def market_data_time(self, inst: Instrument, intent: Intent):
        return inst.book_at

    def calculate(self, s: Session, account: Account, intent: Intent) -> Plan:
        now = self.clock.now
        if intent.synthetic:
            if self.config.mode not in (Mode.OFFLINE, Mode.SHADOW):
                raise RiskRejected("SYNTHETIC_LIVE_SCOPE_FORBIDDEN")
        elif (
            self.config.mode not in (Mode.LIVE_CANARY, Mode.LIVE_LIMITED)
            or not self.config.live_trading_enabled
            or self.store.namespace != "live"
            or (intent.account_id, intent.tournament_id)
            != (self.config.expected_account_id, self.config.expected_tournament_id)
        ):
            raise RiskRejected("LIVE_NOT_AUTHORIZED")
        if account.lifecycle != "HEALTHY":
            raise RiskRejected("RECONCILIATION_REQUIRED_OR_PAUSED")
        if intent.tournament_id != account.tournament or account.id != intent.account_id:
            raise RiskRejected("IDENTITY_MISMATCH")
        if now < intent.created_at or now >= intent.expires_at:
            raise RiskRejected("INTENT_EXPIRED")
        drain = (0 if intent.strategy_id == "mm_sig" and self.config.mm_risk_policy is not None
                 and intent.action == "REDUCE_TO_TARGET" else self.config.drain_seconds)
        if not self.starts_at <= now < self.ends_at or now >= self.ends_at - __import__(
            "datetime"
        ).timedelta(seconds=drain):
            raise RiskRejected("TRADING_CLOSED")
        if (
            account.reconciled_at is None
            or (now - utc_time(account.reconciled_at)).total_seconds()
            > self.config.account_ttl_seconds
        ):
            raise RiskRejected("RECONCILIATION_REQUIRED")
        inst = s.scalar(
            select(Instrument).where(Instrument.id == intent.instrument_id).with_for_update()
        )
        if (
            inst is None
            or inst.tournament != account.tournament
            or inst.outcome != intent.outcome_side
            or inst.exchange_id is None
            or not inst.mapping_evidence
        ):
            raise RiskRejected("INSTRUMENT_MAPPING_UNKNOWN")
        if not inst.reviewed or inst.review_until is None or utc_time(inst.review_until) <= now:
            raise RiskRejected("MARKET_NOT_REVIEWED")
        if (inst.rules_hash, inst.mapping_hash) != (intent.rules_hash, intent.mapping_hash):
            self.store.audit(s,account.id,"DOCUMENT_BINDING_DIFFERENCE",
                {"policy":"REPORT_ONLY","intent":intent.intent_id,"old":[intent.rules_hash,intent.mapping_hash],
                 "new":[inst.rules_hash,inst.mapping_hash]},now)
        if inst.status != "OPEN" or inst.quality != "HEALTHY" or inst.book_at is None:
            raise RiskRejected("DATA_NOT_READY")
        data_at = self.market_data_time(inst, intent)
        now = self.clock.sync()
        if (
            data_at is None
            or not 0 <= (now - utc_time(data_at)).total_seconds() <= self.config.book_ttl_seconds
        ):
            raise RiskRejected("BOOK_STALE")
        if inst.fee_per_share is None or inst.fee_per_share < 0:
            raise RiskRejected("FEE_MODEL_UNVERIFIED")
        if self.config.allowed_instruments and inst.id not in self.config.allowed_instruments:
            raise RiskRejected("INSTRUMENT_NOT_ALLOWED")
        orders = list(s.scalars(select(Order).where(Order.account_id == account.id)))
        from sigbot.uncertainty import check_scope

        check_scope(s, account, inst, self.config.parallel_strategy_execution)
        for existing in s.scalars(
            select(IntentRow).where(
                IntentRow.account_id == account.id,
                IntentRow.instrument_id == inst.id,
                IntentRow.active.is_(True),
            )
        ):
            if existing.id != intent.intent_id and not self.can_coexist(intent, existing.payload):
                raise Conflict("CONFLICTING_INTENT")
        positions = list(s.scalars(select(Position).where(Position.account_id == account.id)))
        if self.config.parallel_strategy_execution and intent.action == "INCREASE_TO_TARGET":
            from sigbot.accounting import lot_origins

            binary_ids = set(s.scalars(select(Instrument.id).where(
                Instrument.tournament == inst.tournament,
                Instrument.exchange_id == inst.exchange_id,
            )))
            for _, origin, _ in lot_origins(s, account.id, binary_ids):
                owner = origin.get("strategy_id")
                if owner != intent.strategy_id and "mm_sig" in {owner, intent.strategy_id}:
                    raise RiskRejected("MM_BINARY_INVENTORY_OWNED_BY_OTHER_STRATEGY")
            for pending in orders:
                if pending.instrument_id in binary_ids and (pending.reserve_cash or pending.reserve_quantity):
                    origin = s.get(IntentRow, pending.intent_id)
                    owner = origin.payload.get("strategy_id") if origin else None
                    if owner != intent.strategy_id and "mm_sig" in {owner, intent.strategy_id}:
                        raise RiskRejected("MM_BINARY_RESERVED_BY_OTHER_STRATEGY")
        if self.config.parallel_strategy_execution and intent.strategy_id != "manual_target":
            # A live SSR basket owns both outcome directions of both exchanges.
            # An opposite-side order would net existing shares at the venue.
            ssr_owners = []
            for basket in s.scalars(
                select(StrategyEvent).where(
                    StrategyEvent.account_id == account.id,
                    StrategyEvent.strategy_id == "ssr_sig",
                    StrategyEvent.state != "CLOSED",
                )
            ):
                owned = [s.get(Instrument, key) for key in basket.data.get("instrument_ids", [])]
                if any(i is not None and i.exchange_id == inst.exchange_id for i in owned):
                    if intent.strategy_id != "ssr_sig":
                        raise RiskRejected("SSR_EXCHANGE_RESERVED_BY_ACTIVE_PAIR")
                    if any(
                        i is not None
                        and i.exchange_id == inst.exchange_id
                        and i.outcome == inst.outcome
                        for i in owned
                    ):
                        ssr_owners.append(basket)
            aliases = {
                i.id
                for i in s.scalars(
                    select(Instrument).where(
                        Instrument.tournament == inst.tournament,
                        Instrument.exchange_id == inst.exchange_id,
                        Instrument.outcome == inst.outcome,
                    )
                )
            }
            owners = list(
                s.scalars(
                    select(StrategyEvent).where(
                        StrategyEvent.account_id == account.id,
                        StrategyEvent.instrument_id.in_(aliases),
                        StrategyEvent.state != "CLOSED",
                    )
                )
            )
            owners.extend(ssr_owners)
            if any(e.strategy_id != intent.strategy_id for e in owners):
                raise RiskRejected("REAL_ASSET_OWNED_BY_OTHER_STRATEGY")
            for order in orders:
                if order.instrument_id in aliases and (
                    order.reserve_cash or order.reserve_quantity
                ):
                    origin = s.get(IntentRow, order.intent_id)
                    assert origin is not None
                    if origin.payload.get("strategy_id") != intent.strategy_id:
                        raise RiskRejected("REAL_ASSET_RESERVED_BY_OTHER_STRATEGY")
            if any(p.quantity and p.instrument_id in aliases for p in positions) and not owners:
                raise RiskRejected("REAL_ASSET_FOREIGN_INVENTORY_PROTECTED")
        pos = next((p for p in positions if p.instrument_id == inst.id), None)
        current = pos.quantity if pos else ZERO
        pending_buy = sum(
            (
                o.quantity - o.filled
                for o in orders
                if o.instrument_id == inst.id and o.side == "BUY" and o.reserve_cash > 0
            ),
            ZERO,
        )
        pending_sell = sum((o.reserve_quantity for o in orders if o.instrument_id == inst.id), ZERO)
        if intent.action == "INCREASE_TO_TARGET":
            side = "BUY"
            quantity = max(ZERO, intent.target_quantity - current - pending_buy)
        else:
            side = "SELL"
            quantity = max(ZERO, current - intent.target_quantity - pending_sell)
            self.check_reduce_pending_buy(intent, pending_buy)
            if quantity > current - pending_sell or (pos and pos.cost is None):
                raise RiskRejected("REDUCE_ONLY_POSITION_UNKNOWN")
        fee = inst.fee_per_share * quantity
        reserve = quantity * intent.limit_price + fee if side == "BUY" else ZERO
        plan = Plan(
            intent.intent_id,
            quantity,
            side,
            intent.limit_price,
            reserve,
            quantity if side == "SELL" else ZERO,
            fee,
            now,
            inst.rules_hash,
            inst.mapping_hash,
        )
        if intent.strategy_id == "mm_sig":
            from sigbot.mm_risk import apply_plan

            plan = apply_plan(self, s, account, intent, plan)
            quantity, fee, reserve = plan.quantity, plan.fee_bound, plan.reserve_cash
        self.check_capital(s, account, intent, side, reserve)
        if not quantity:
            return plan
        check_grid(intent.limit_price, quantity, inst.tick, inst.step)
        self.check_liquidity(inst, side, intent.limit_price, quantity, intent)
        limits = self.config.risk
        if self.config.mode == Mode.LIVE_CANARY and any(
            v is None for v in limits.model_dump().values()
        ):
            raise RiskRejected("RISK_LIMITS_UNCONFIGURED")
        active = [o for o in orders if o.reserve_cash > 0 or o.reserve_quantity > 0]
        strategy_events = list(
            s.scalars(select(StrategyEvent).where(StrategyEvent.account_id == account.id))
        )
        future_fees = sum((e.future_fee_reserve for e in strategy_events), ZERO)
        cash_floor = max(
            (
                Decimal(e.data.get("cash_floor", "0"))
                for e in strategy_events
                if e.state != "CLOSED"
            ),
            default=ZERO,
        )
        total_reserve = sum((o.reserve_cash for o in active), ZERO) + future_fees
        costs = [p.cost for p in positions]
        if any(c is None for c in costs):
            raise RiskRejected("UNKNOWN_COST_BASIS")
        exposure = sum((c for c in costs if c is not None), ZERO) + total_reserve + reserve
        ids = {
            i.id
            for i in s.scalars(select(Instrument).where(Instrument.event_group == inst.event_group))
        }
        event_exposure = sum((p.cost or ZERO for p in positions if p.instrument_id in ids), ZERO)
        event_exposure += (
            sum((o.reserve_cash for o in orders if o.instrument_id in ids), ZERO) + reserve
        )
        event_exposure += sum(
            (e.future_fee_reserve for e in strategy_events if e.instrument_id in ids), ZERO
        )
        notional = quantity * intent.limit_price + fee
        policies = [
            e.data["account_limits"]
            for e in strategy_events
            if e.state != "CLOSED" and e.data.get("account_limits")
        ]
        total_caps = [
            v
            for v in [
                limits.max_total_exposure,
                *(Decimal(p["total_cost"]) for p in policies if p.get("total_cost") is not None),
            ]
            if v is not None
        ]
        event_caps = [
            v
            for v in [
                limits.max_event_exposure,
                *(Decimal(p["event_cost"]) for p in policies if p.get("event_cost") is not None),
            ]
            if v is not None
        ]
        slot_caps = [
            v
            for v in [
                limits.max_open_orders,
                *(int(p["events"]) for p in policies if p.get("events") is not None),
            ]
            if v is not None
        ]
        inst_by_id = {i.id: i for i in s.scalars(select(Instrument))}
        occupied_events = {inst_by_id[p.instrument_id].event_group for p in positions if p.quantity}
        occupied_events.update(inst_by_id[o.instrument_id].event_group for o in active)
        occupied_events.update(
            inst_by_id[e.instrument_id].event_group for e in strategy_events if e.state != "CLOSED"
        )
        if (
            notional > intent.max_order_notional
            or limits.max_order_notional is not None
            and notional > limits.max_order_notional
            or side == "BUY"
            and total_caps
            and exposure > min(total_caps)
            or side == "BUY"
            and event_caps
            and event_exposure > min(event_caps)
            or side == "BUY"
            and slot_caps
            and len(occupied_events | {inst.event_group}) > min(slot_caps)
            or side == "BUY"
            and limits.max_instrument_quantity is not None
            and current + pending_buy + quantity > limits.max_instrument_quantity
            or limits.max_open_orders is not None
            and len(active) >= limits.max_open_orders
            or reserve > account.cash - total_reserve
            or (side == "BUY" and reserve > account.cash - total_reserve - cash_floor)
        ):
            raise RiskRejected("RISK_LIMIT_EXCEEDED")
        return plan

    def preview(self, intent: Intent) -> Plan:
        with self.store.transaction() as s:
            account = self.store.lock_account(s, intent.account_id)
            return self.calculate(s, account, intent)

    def check_capital(self, session, account, intent, side, reserve, *, exclude_order=None):
        principal = self.config.strategy_capital_allocations.get(intent.strategy_id)
        if side != "BUY" or principal is None:
            return
        from sigbot.accounting import strategy_finances

        funds = strategy_finances(
            session, account.id, intent.strategy_id, principal, exclude_order=exclude_order
        )
        if reserve > funds["available"]:
            raise RiskRejected("STRATEGY_CAPITAL_EXCEEDED")

    def reserve(self, intent: Intent) -> str | None:
        with self.store.transaction() as s:
            account = self.store.lock_account(s, intent.account_id)
            return self.reserve_locked(s, account, intent)

    def reserve_locked(self, s: Session, account: Account, intent: Intent) -> str | None:
        """Same OMS reservation within an allocator's existing account transaction."""
        old = s.get(IntentRow, intent.intent_id)
        if old:
            if old.digest != intent.digest or old.account_id != account.id:
                raise Conflict("INTENT_CONTENT_CONFLICT")
            return s.scalar(select(Order.id).where(Order.intent_id == intent.intent_id))
        plan = self.calculate(s, account, intent)
        if plan.risk_decision is not None:
            self.store.audit(s, account.id, "MM_RISK_SIZING_DECISION", {
                "intent_id": intent.intent_id, **plan.risk_decision}, self.clock.now)
        s.add(
            IntentRow(
                id=intent.intent_id,
                account_id=account.id,
                instrument_id=intent.instrument_id,
                digest=intent.digest,
                payload=intent.model_dump(mode="json"),
                active=bool(plan.quantity),
            )
        )
        s.flush()
        if not plan.quantity:
            return None
        key = ("SIM-LOCAL-" if intent.synthetic else "SIG-LOCAL-") + str(uuid4())
        s.add(
            Order(
                id=key,
                intent_id=intent.intent_id,
                account_id=account.id,
                instrument_id=intent.instrument_id,
                side=plan.side,
                quantity=plan.quantity,
                price=plan.price,
                fee_bound=plan.fee_bound,
                reserve_cash=plan.reserve_cash,
                reserve_quantity=plan.reserve_quantity,
            )
        )
        s.flush()
        s.add(Outbox(order_id=key))
        if intent.strategy_id == "mm_sig" and self.config.mm_risk_policy is not None:
            from sigbot.mm_risk import reserved

            reserved(s, account, intent, plan, key, self.config.mm_risk_policy, self.clock.now)
        self.on_reserved(s, account, intent, plan, key)
        self.store.audit(
            s,
            account.id,
            "INTENT_RESERVED",
            {
                "intent": intent.intent_id,
                "order": key,
                "quantity": str(plan.quantity),
                "cash": str(plan.reserve_cash),
                "rules_hash": plan.rules_hash,
                "mapping_hash": plan.mapping_hash,
            },
            self.clock.now,
        )
        return key

    def on_reserved(
        self, s: Session, account: Account, intent: Intent, plan: Plan, order_id: str
    ) -> None:
        """Optional strategy state, committed atomically with the common reservation."""

    def can_coexist(self, intent: Intent, existing: dict) -> bool:
        return False

    def check_reduce_pending_buy(self, intent: Intent, pending_buy: Decimal) -> None:
        if pending_buy:
            raise RiskRejected("REDUCE_ONLY_POSITION_UNKNOWN")

    def check_liquidity(self, inst, side, price, quantity, intent: Intent) -> None:
        depth = executable_depth(inst.book, side, price)
        if depth is None or depth < quantity:
            raise RiskRejected("INSUFFICIENT_EXECUTABLE_DEPTH")

    def check_dispatch(self, s: Session, account: Account, intent: Intent, order: Order) -> None:
        """Optional additional strategy checks in the sole executor's send transaction."""

    def on_observed(self, s: Session, account: Account, order: Order, observation) -> None:
        """Optional lifecycle projection after the shared ledger ingests fills."""

    def on_unsent_expired(self, s: Session, account: Account, intent: Intent, order: Order) -> None:
        """Release strategy-only reserves after a proven unsent expiry."""

    def on_unsent_canceled(
        self, s: Session, account: Account, intent: Intent, order: Order
    ) -> None:
        """Optional strategy projection after local cancellation of a never-sent order."""
