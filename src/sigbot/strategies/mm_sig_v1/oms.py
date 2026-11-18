"""Passive policy inside the existing account transaction and sole Executor."""

import json
from datetime import timedelta
from decimal import Decimal

from sqlalchemy import select

from sigbot.domain import ZERO, Intent, content_hash
from sigbot.errors import ContractNotVerified, RiskRejected
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
from sigbot.strategies.mm_sig_v1.core import check_book, quote
from sigbot.strategies.mm_sig_v1.models import Book, Candidate, Inventory, MmConfig


def metadata(intent: Intent) -> MmConfig:
    if (intent.strategy_id, intent.strategy_version) != ("mm_sig", "1.0"):
        raise RiskRejected("MM_STRATEGY_SCOPE_MISMATCH")
    if not intent.synthetic:
        raise ContractNotVerified("MM_LIVE_CALIBRATION_AND_AUTHORIZATION_NOT_VERIFIED")
    data = json.loads(intent.note or "{}")
    if data.get("kind") != "PASSIVE_QUOTE" or not data.get("round_id"):
        raise RiskRejected("MM_QUOTE_METADATA_REQUIRED")
    return MmConfig.model_validate(data["config"])


def candidate(inst: Instrument) -> Candidate:
    return Candidate.model_validate(
        {
            **inst.mapping_evidence["mm_candidate"],
            "instrument_id": inst.id,
            "event_key": inst.event_group,
            "rules_hash": inst.rules_hash,
            "mapping_hash": inst.mapping_hash,
            "reviewed": inst.reviewed,
            "review_until": inst.review_until,
            "status": inst.status,
            "tick": inst.tick,
            "step": inst.step,
            "fee_per_share": inst.fee_per_share,
            "book": {**inst.book, "received_at": inst.book_at, "quality": inst.quality},
        }
    )


def make_intent(account_id, tournament_id, c, inv, proposal, cfg, now, round_id) -> Intent:
    side = proposal.side
    return Intent.model_validate(
        {
            "schema_version": "1.0",
            "intent_id": "SIM-MM-" + content_hash([account_id, c.instrument_id, round_id, side]),
            "synthetic": True,
            "account_id": account_id,
            "tournament_id": tournament_id,
            "instrument_id": c.instrument_id,
            "outcome_side": "YES",
            "action": "INCREASE_TO_TARGET" if side == "BUY" else "REDUCE_TO_TARGET",
            "target_quantity": inv.quantity + proposal.quantity
            if side == "BUY"
            else inv.quantity - proposal.quantity,
            "limit_price": proposal.price,
            "max_order_notional": cfg.max_order_cost,
            "created_at": now,
            "expires_at": now + timedelta(seconds=cfg.quote_seconds),
            "rules_hash": c.rules_hash,
            "mapping_hash": c.mapping_hash,
            "strategy_id": "mm_sig",
            "strategy_version": "1.0",
            "note": json.dumps(
                {
                    "kind": "PASSIVE_QUOTE",
                    "round_id": round_id,
                    "config": cfg.model_dump(mode="json"),
                },
                sort_keys=True,
            ),
        }
    )


class MmPlanner(Planner):
    def event(self, s, account_id, event_key):
        return s.get(StrategyEvent, (account_id, "mm_sig", "1.0", event_key))

    def inventory(self, s, account, inst):
        pos = s.get(Position, (account.id, inst.id))
        qty, cost = (pos.quantity, pos.cost) if pos else (ZERO, ZERO)
        if cost is None:
            raise RiskRejected("MM_COST_UNKNOWN")
        if qty:
            lots = list(
                s.scalars(
                    select(PositionLot).where(
                        PositionLot.account_id == account.id, PositionLot.instrument_id == inst.id
                    )
                )
            )
            if sum((lot.quantity for lot in lots), ZERO) != qty:
                raise RiskRejected("MM_INVENTORY_PROVENANCE_UNKNOWN")
            for lot in lots:
                if not lot.quantity:
                    continue
                fill = s.get(FillRow, (account.id, lot.economic_id.removeprefix("fill:")))
                order = s.get(Order, fill.order_id) if fill else None
                owner = s.get(IntentRow, order.intent_id) if order else None
                if owner is None or (
                    owner.payload.get("strategy_id"),
                    owner.payload.get("strategy_version"),
                ) != ("mm_sig", "1.0"):
                    raise RiskRejected("MM_FOREIGN_INVENTORY")
        event = self.event(s, account.id, inst.event_group)
        return Inventory(
            quantity=qty, cost=cost, first_fill_at=event.holding_started_at if event else None
        )

    def can_coexist(self, intent, existing):
        return (existing.get("strategy_id"), existing.get("strategy_version")) == (
            "mm_sig",
            "1.0",
        ) and existing.get("action") != intent.action

    def check_reduce_pending_buy(self, intent, pending_buy):
        metadata(intent)  # Covered SELL uses already-owned inventory, never a pending BUY.

    def check_liquidity(self, inst, side, price, quantity, intent):
        cfg = metadata(intent)
        bid, ask = check_book(
            Book.model_validate(
                {
                    **inst.book,
                    "received_at": inst.book_at,
                    "quality": inst.quality,
                }
            ),
            self.clock.now,
            cfg,
        )
        if side == "BUY" and price >= ask or side == "SELL" and price <= bid:
            raise RiskRejected("MM_WOULD_TAKE_LIQUIDITY")

    def guard(self, s, account, inst, cfg, side, price, quantity, exclude=None):
        inv = self.inventory(s, account, inst)
        events = list(
            s.scalars(select(StrategyEvent).where(StrategyEvent.account_id == account.id))
        )
        for event in events:
            if (
                event.event_key == inst.event_group
                and event.state != "CLOSED"
                and (event.strategy_id != "mm_sig" or event.instrument_id != inst.id)
            ):
                raise RiskRejected("MM_EVENT_ALREADY_OWNED")
        event = self.event(s, account.id, inst.event_group)
        if (
            event
            and event.state != "CLOSED"
            and event.data.get("config") != cfg.model_dump(mode="json")
        ):
            raise RiskRejected("MM_ACTIVE_POLICY_CHANGED")
        if (
            event
            and event.state != "CLOSED"
            and event.data.get("fee_per_share") != str(inst.fee_per_share)
        ):
            raise RiskRejected("MM_ACTIVE_FEE_MODEL_CHANGED")
        active = [
            o
            for o in s.scalars(select(Order).where(Order.account_id == account.id))
            if o.id != exclude and (o.reserve_cash or o.reserve_quantity)
        ]
        if any(
            o.request_state in {"UNKNOWN", "SENDING"} or o.cancel_state == "CANCEL_UNKNOWN"
            for o in active
        ):
            raise RiskRejected("MM_UNKNOWN_ORDER")
        own = [o for o in active if o.instrument_id == inst.id]
        instruments = {i.id: i for i in s.scalars(select(Instrument))}
        if any(
            p.quantity
            and p.instrument_id != inst.id
            and instruments[p.instrument_id].event_group == inst.event_group
            for p in s.scalars(select(Position).where(Position.account_id == account.id))
        ) or any(
            o.instrument_id != inst.id
            and instruments[o.instrument_id].event_group == inst.event_group
            for o in active
        ):
            raise RiskRejected("MM_EVENT_ALREADY_OWNED")
        if any(
            (side == "BUY" and o.side == "SELL" and price >= o.price)
            or (side == "SELL" and o.side == "BUY" and price <= o.price)
            for o in own
        ):
            raise RiskRejected("MM_SELF_CROSS")
        pending = sum((o.quantity - o.filled for o in own if o.side == "BUY"), ZERO)
        if side == "BUY" and inv.quantity + pending + quantity > cfg.max_inventory:
            raise RiskRejected("MM_INVENTORY_LIMIT")
        if side == "SELL" and quantity > inv.quantity - sum(
            (o.reserve_quantity for o in own), ZERO
        ):
            raise RiskRejected("MM_UNCOVERED_SELL")
        return inv

    def calculate(self, s, account, intent):
        cfg = metadata(intent)
        inst = s.get(Instrument, intent.instrument_id)
        if inst is None or "mm_candidate" not in inst.mapping_evidence:
            raise RiskRejected("MM_CANDIDATE_REQUIRED")
        inv = self.inventory(s, account, inst)
        side = "BUY" if intent.action == "INCREASE_TO_TARGET" else "SELL"
        proposals = quote(candidate(inst), inv, self.clock.now, cfg)
        proposed = next((q for q in proposals if q.side == side), None)
        if proposed is None or proposed.price != intent.limit_price:
            raise RiskRejected("MM_QUOTE_CHANGED_OR_REDUCE_ONLY")
        plan = super().calculate(s, account, intent)
        if plan.quantity > proposed.quantity:
            raise RiskRejected("MM_QUOTE_SIZE_CHANGED")
        self.guard(s, account, inst, cfg, side, plan.price, plan.quantity)
        if plan.quantity * (plan.price + inst.fee_per_share) > cfg.max_order_cost:
            raise RiskRejected("MM_ORDER_COST_LIMIT")
        if side == "BUY":
            positions = list(s.scalars(select(Position).where(Position.account_id == account.id)))
            orders = list(s.scalars(select(Order).where(Order.account_id == account.id)))
            events = list(
                s.scalars(select(StrategyEvent).where(StrategyEvent.account_id == account.id))
            )
            instruments = {i.id: i for i in s.scalars(select(Instrument))}
            future = sum((e.future_fee_reserve for e in events), ZERO)
            reserve = sum((o.reserve_cash for o in orders), ZERO) + future
            extra = plan.reserve_cash + plan.fee_bound
            event_cost = sum(
                (
                    p.cost or ZERO
                    for p in positions
                    if instruments[p.instrument_id].event_group == inst.event_group
                ),
                ZERO,
            )
            event_cost += sum(
                (
                    o.reserve_cash
                    for o in orders
                    if instruments[o.instrument_id].event_group == inst.event_group
                ),
                ZERO,
            )
            event_cost += sum(
                (e.future_fee_reserve for e in events if e.event_key == inst.event_group), ZERO
            )
            occupied = {instruments[p.instrument_id].event_group for p in positions if p.quantity}
            occupied.update(
                instruments[o.instrument_id].event_group
                for o in orders
                if o.reserve_cash or o.reserve_quantity
            )
            occupied.update(e.event_key for e in events if e.state != "CLOSED")
            if (
                extra > account.cash - reserve
                or event_cost + extra > cfg.max_event_cost
                or sum((p.cost or ZERO for p in positions), ZERO) + reserve + extra
                > cfg.max_total_cost
                or len(occupied | {inst.event_group}) > cfg.max_events
            ):
                raise RiskRejected("MM_SHARED_ACCOUNT_COST_LIMIT")
        return plan

    def on_reserved(self, s, account, intent, plan, order_id):
        cfg = metadata(intent)
        inst = s.get(Instrument, intent.instrument_id)
        event = self.event(s, account.id, inst.event_group)
        if event is None:
            event = StrategyEvent(
                account_id=account.id,
                strategy_id="mm_sig",
                strategy_version="1.0",
                event_key=inst.event_group,
                instrument_id=inst.id,
                state="QUOTING",
                state_group="SYNTHETIC",
                shock_group=inst.event_group,
                direction_bucket="YES",
                future_fee_reserve=ZERO,
                data={},
            )
            s.add(event)
        event.state = "QUOTING"
        event.data = {
            **event.data,
            "config": cfg.model_dump(mode="json"),
            "fee_per_share": str(inst.fee_per_share),
            "account_limits": {
                "total_cost": str(cfg.max_total_cost),
                "event_cost": str(cfg.max_event_cost),
                "events": cfg.max_events,
            },
        }
        if plan.side == "BUY":
            event.future_fee_reserve += plan.fee_bound
        bid, ask = check_book(candidate(inst).book, self.clock.now, cfg)
        self.store.audit(
            s,
            account.id,
            "MM_QUOTE_RESERVED",
            {
                "strategy_id": "mm_sig",
                "order_id": order_id,
                "event_key": inst.event_group,
                "instrument_id": inst.id,
                "rules_hash": inst.rules_hash,
                "mapping_hash": inst.mapping_hash,
                "sequence": inst.book["sequence"],
                "source_at": inst.book["source_at"],
                "reference_bid": str(bid),
                "reference_ask": str(ask),
                "quote_distance": str(bid - plan.price if plan.side == "BUY" else plan.price - ask),
                "outcome": inst.outcome,
            },
            self.clock.now,
        )

    def check_dispatch(self, s, account, intent, order):
        cfg = metadata(intent)
        inst = s.get(Instrument, intent.instrument_id)
        inv = self.guard(s, account, inst, cfg, order.side, order.price, order.quantity, order.id)
        proposed = next(
            (q for q in quote(candidate(inst), inv, self.clock.now, cfg) if q.side == order.side),
            None,
        )
        if proposed is None or proposed.price != order.price or order.quantity > proposed.quantity:
            raise RiskRejected("MM_QUOTE_CHANGED_OR_REDUCE_ONLY")

    def on_observed(self, s, account, order, observation):
        inst = s.get(Instrument, order.instrument_id)
        event = self.event(s, account.id, inst.event_group)
        pos = s.get(Position, (account.id, inst.id))
        quantity = pos.quantity if pos else ZERO
        if quantity and event.holding_started_at is None:
            at = [f.filled_at for f in observation.fills if f.filled_at is not None]
            event.holding_started_at = min(at) if at else self.clock.now
        if not quantity:
            event.holding_started_at = None
        records = dict(event.data.get("quotes", {}))
        record = dict(records.get(order.id, {}))
        record.setdefault("acknowledged_at", self.clock.now.isoformat())
        if order.request_state == "RESOLVED":
            record.setdefault("terminal_at", self.clock.now.isoformat())
        records[order.id] = record
        event.data = {
            **event.data,
            "quotes": records,
            "peak_inventory": str(max(Decimal(event.data.get("peak_inventory", "0")), quantity)),
            "peak_inventory_cost": str(
                max(Decimal(event.data.get("peak_inventory_cost", "0")), pos.cost if pos else ZERO)
            ),
        }
        active = list(
            s.scalars(
                select(Order).where(Order.account_id == account.id, Order.instrument_id == inst.id)
            )
        )
        pending = sum(
            (o.quantity - o.filled for o in active if o.side == "BUY" and o.reserve_cash), ZERO
        )
        event.future_fee_reserve = (quantity + pending) * Decimal(event.data["fee_per_share"])
        event.state = (
            "QUOTING"
            if any(o.reserve_cash or o.reserve_quantity for o in active)
            else "INVENTORY"
            if quantity
            else "CLOSED"
        )
        if event.state == "CLOSED":
            event.last_exit_at = self.clock.now

    def on_unsent_expired(self, s, account, intent, order):
        inst = s.get(Instrument, intent.instrument_id)
        event = self.event(s, account.id, inst.event_group)
        if order.side == "BUY":
            event.future_fee_reserve = max(ZERO, event.future_fee_reserve - order.fee_bound)
        pos = s.get(Position, (account.id, inst.id))
        active = any(
            o.reserve_cash or o.reserve_quantity
            for o in s.scalars(
                select(Order).where(Order.account_id == account.id, Order.instrument_id == inst.id)
            )
        )
        event.state = "QUOTING" if active else "INVENTORY" if pos and pos.quantity else "CLOSED"
        records = dict(event.data.get("quotes", {}))
        records[order.id] = {"terminal_at": self.clock.now.isoformat(), "never_sent": True}
        event.data = {**event.data, "quotes": records}

    def reserve_quotes(self, account_id, instrument_id, cfg, round_id):
        with self.store.transaction() as s:
            account = self.store.lock_account(s, account_id)
            inst = s.get(Instrument, instrument_id)
            if inst is None:
                raise RiskRejected("MM_INSTRUMENT_UNKNOWN")
            previous = {}
            for side in ("BUY", "SELL"):
                key = "SIM-MM-" + content_hash([account_id, instrument_id, round_id, side])
                row = s.get(IntentRow, key)
                if row:
                    if metadata(Intent.model_validate(row.payload)) != cfg:
                        raise RiskRejected("MM_ROUND_CONFIG_CONFLICT")
                    previous[side] = s.scalar(select(Order.id).where(Order.intent_id == key))
            if previous:
                return previous
            inv = self.inventory(s, account, inst)
            c = candidate(inst)
            proposals = quote(c, inv, self.clock.now, cfg)
            # Both local reservations commit together; remote acceptance is never atomic.
            return {
                q.side: self.reserve_locked(
                    s,
                    account,
                    make_intent(
                        account_id, account.tournament, c, inv, q, cfg, self.clock.now, round_id
                    ),
                )
                for q in proposals
            }

    def on_unsent_canceled(self, s, account, intent, order):
        self.on_unsent_expired(s, account, intent, order)
