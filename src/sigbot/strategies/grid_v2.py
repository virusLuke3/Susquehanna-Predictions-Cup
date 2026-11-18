"""Risk sized grid v2 policy. Reservation and dispatch use the common account lock."""

from decimal import Decimal as D

from sqlalchemy import select

from sigbot.domain import ZERO, Intent, utc_time
from sigbot.errors import RiskRejected
from sigbot.storage import Instrument, IntentRow, Order, StrategyEvent
from sigbot.strategies.grid_risk import (
    IDENTITY,
    Parameters,
    daily_gate,
    family_projection,
    frozen_plan,
    plan_key,
    size_order,
)
from sigbot.strategies.grid_sig import GridPlanner, finances


class RiskGridPlanner(GridPlanner):
    identity = IDENTITY
    parameter_class = Parameters
    risk_reasons = {"STOP", "TIMEOUT", "MODEL_BREAK"}

    def rebuild_intent(self, session, account, intent):
        n = self.envelope(intent)
        if n["kind"] == "ENTRY" and n.get("execution"):
            # Keep the original bounded limit; never chase a refreshed ask.
            return intent
        return super().rebuild_intent(session, account, intent)

    def check_liquidity(self, inst, side, price, quantity, intent):
        n = self.envelope(intent)
        if side == "BUY" and n.get("execution"):
            from sigbot.strategies.grid_execution import check_entry_execution

            book = check_entry_execution(
                inst,
                n["plan"],
                n["slot"],
                n["execution"],
                price,
                self.clock.now,
                self.config.book_ttl_seconds,
            )
            if n["execution"]["action"] == "ACTIVE_LIMIT":
                depth = sum((q for p, q in book[1] if p <= price), ZERO)
                if quantity > depth:
                    raise RiskRejected("GRID_ACTIVE_ASK_DEPTH_INSUFFICIENT")
            return
        super().check_liquidity(inst, side, price, quantity, intent)

    def validate_entry(self, session, account, intent, price, quantity, *, dispatch=False):
        n = self.envelope(intent)
        p = Parameters.model_validate(n["parameters"])
        inst = session.get(Instrument, intent.instrument_id)
        f, plan = n["profile"], n["plan"]
        if n.get("cycle_id") != plan_key(plan) or plan != frozen_plan(
            n["frozen_profile"], inst.tick, p
        ):
            raise RiskRejected("GRID_V2_FROZEN_CONTRACT_CHANGED")
        slot = n.get("slot")
        if not isinstance(slot, int) or not 1 <= slot <= len(plan["levels"]):
            raise RiskRejected("GRID_V2_SLOT_NOT_ENABLED")
        if (
            price < D(plan["stop"]) + inst.tick
            or price > D(plan["levels"][slot - 1])
            or D(plan["target"]) < price + inst.tick
        ):
            raise RiskRejected("GRID_V2_ENTRY_OUTSIDE_FROZEN_PLAN")
        if quantity > D(n["quantity_cap"]):
            raise RiskRejected("GRID_V2_QUANTITY_MAY_ONLY_SHRINK")
        if n.get("execution"):
            from sigbot.strategies.grid_execution import check_entry_execution, order_seconds

            order_seconds(n, p.order_seconds)
            check_entry_execution(
                inst,
                plan,
                slot,
                n["execution"],
                price,
                self.clock.now,
                self.config.book_ttl_seconds,
            )
        if intent.action != "INCREASE_TO_TARGET":
            raise RiskRejected("GRID_V2_BUY_ACTION_REQUIRED")
        if (
            not f.get("eligible")
            or not 0
            <= (self.clock.now - utc_time(f["latest_end"])).total_seconds()
            <= p.history_max_age_seconds
        ):
            raise RiskRejected("GRID_HISTORY_STALE_OR_INELIGIBLE")
        if not 0 <= (self.clock.now - utc_time(f["asof"])).total_seconds() <= 600:
            raise RiskRejected("GRID_PROFILE_EXPIRED")
        if f.get("drift") is not None and D(f["drift"]) <= D("-.01"):
            raise RiskRejected("GRID_PERSISTENT_ADVERSE_DRIFT")
        event = self.event(session, account.id, inst.exchange_id)
        if (
            event
            and event.data.get("entry_replacement", {}).get("phase") in {"WAIT_CANCEL", "READY"}
            and not n.get("replacement_of")
        ):
            raise RiskRejected("GRID_REPLACEMENT_SOURCE_REQUIRED")
        if source_id := n.get("replacement_of"):
            from sigbot.strategies.grid_execution import replacement_state

            pending = event.data.get("entry_replacement", {}) if event else {}
            parent = session.get(Order, source_id)
            if (
                pending.get("source_order_id") != source_id
                or pending.get("phase") not in {"READY", "SUBMITTED"}
                or pending.get("slot") != slot
                or pending.get("expires_at") != n.get("replacement_deadline")
                or n.get("execution", {}).get("action") != "ACTIVE_LIMIT"
                or parent is None
                or parent.account_id != account.id
                or parent.instrument_id != inst.id
                or quantity > parent.quantity
                or replacement_state(parent, pending, self.clock.now) != "READY"
                or pending.get("intent_id") not in {None, intent.intent_id}
            ):
                raise RiskRejected("GRID_REPLACEMENT_NOT_READY")
        if event and event.state == "CLOSED":
            rearm = event.data.get("rearm", {}).get(inst.outcome)
            if rearm and not rearm.get("ready"):
                raise RiskRejected("GRID_V2_WAIT_REAL_PRICE_REARM")
        if event and event.state != "CLOSED":
            if (
                event.data.get("cycle_id") != n["cycle_id"]
                or event.data.get("risk_exit")
                or event.data.get("exit_started")
            ):
                raise RiskRejected("GRID_V2_CYCLE_EXIT_OR_CHANGED")
            if (
                event.holding_started_at
                and (self.clock.now - utc_time(event.holding_started_at)).total_seconds()
                >= plan["holding_seconds"]
            ):
                raise RiskRejected("GRID_V2_HOLDING_DEADLINE")
        # Ownership is version specific, including active legacy cycles and pending intents.
        for other in session.scalars(
            select(StrategyEvent).where(
                StrategyEvent.account_id == account.id, StrategyEvent.state != "CLOSED"
            )
        ):
            if (other.strategy_id, other.strategy_version) != self.identity:
                ids = [other.instrument_id, *other.data.get("instrument_ids", [])]
                if any(
                    (i := session.get(Instrument, key)) is not None
                    and i.exchange_id == inst.exchange_id
                    for key in ids
                ):
                    raise RiskRejected("GRID_V2_FOREIGN_CYCLE_PROTECTED")
        active = self.own_orders(session, account.id, active=True)
        if any(
            o.side == "BUY" and o.instrument_id == inst.id and o.intent_id != intent.intent_id
            for o, _ in active
        ):
            raise RiskRejected("GRID_V2_BUY_ALREADY_RESTING")
        if event and slot in event.data.get("used_slots", []):
            raise RiskRejected("GRID_V2_FILLED_SLOT_NOT_REUSABLE")
        # E1 evidence comes from persisted, versioned local observations, never a producer boolean.
        evidence = event.data.get("e1", {}).get(inst.outcome, []) if event else []
        from sigbot.strategies.grid_v2_runtime import e1_gate

        if not e1_gate(evidence, self.clock.now, inst.tick, plan):
            raise RiskRejected("GRID_V2_E1_MOVEMENT_NOT_OBSERVED")
        if not daily_gate(session, account.id, inst, self.clock.now, p):
            raise RiskRejected("GRID_V2_DAILY_LOSS_LATCHED")
        projection = family_projection(session, account.id)
        available = finances(session, account.id, p)["available"]
        if dispatch:
            # Exclude exactly this already-reserved request, never all same-price orders.
            own = session.scalar(
                select(Order).where(
                    Order.account_id == account.id, Order.intent_id == intent.intent_id
                )
            )
            if own is None:
                raise RiskRejected("GRID_V2_DISPATCH_RESERVATION_REQUIRED")
            from sigbot.strategies.grid_risk import unit_risk

            m = projection[inst.exchange_id]
            r = own.reserve_cash / own.price * unit_risk(own.price, plan)
            m["cash"] -= own.reserve_cash
            m["risk"] -= r
            m["slots"][(n["cycle_id"], slot)] -= r
            available += own.reserve_cash
        from sigbot.strategies.grid_v2_runtime import capacity

        maximum = size_order(
            plan,
            slot,
            price,
            projection,
            inst.exchange_id,
            available,
            capacity(
                inst,
                plan,
                self.clock.now,
                self.config.book_ttl_seconds,
                price if n.get("execution", {}).get("action") == "ACTIVE_LIMIT" else None,
            ),
            p,
        )
        if quantity > maximum:
            raise RiskRejected("GRID_V2_RISK_OR_CASH_OR_CAPACITY_EXCEEDED")

    def on_reserved(self, session, account, intent, plan, order_id):
        n = self.envelope(intent)
        inst = session.get(Instrument, intent.instrument_id)
        event = self.event(session, account.id, inst.exchange_id)
        saved = dict(event.data) if event else {}
        super().on_reserved(session, account, intent, plan, order_id)
        event = self.event(session, account.id, inst.exchange_id)
        if n["kind"] == "ENTRY":
            event.data = {
                **saved,
                "mu": n["mu"],
                "width": n["width"],
                "cycle_id": n["cycle_id"],
                "plan": n["plan"],
                "frozen_profile": n["frozen_profile"],
                "risk_exit": None,
                "exit_started": False,
                "break_watch": saved.get("break_watch")
                if saved.get("cycle_id") == n["cycle_id"]
                else None,
                "used_slots": saved.get("used_slots", [])
                if saved.get("cycle_id") == n["cycle_id"]
                else [],
            }
            if n.get("replacement_of"):
                event.data = {
                    **event.data,
                    "entry_replacement": {
                        **saved["entry_replacement"],
                        "phase": "SUBMITTED",
                        "intent_id": intent.intent_id,
                        "order_id": order_id,
                    },
                }

    def update_event(self, session, account, order):
        inst = session.get(Instrument, order.instrument_id)
        event = self.event(session, account.id, inst.exchange_id)
        if event is None:
            return
        was_closed = event.state == "CLOSED"
        meta = dict(event.data)
        n = self.envelope(Intent.model_validate(session.get(IntentRow, order.intent_id).payload))
        if order.filled > ZERO:
            if order.side == "SELL":
                meta["exit_started"] = True
            else:
                meta["used_slots"] = sorted(set(meta.get("used_slots", [])) | {n["slot"]})
        event.data = meta
        super().update_event(session, account, order)
        pending = event.data.get("entry_replacement", {})
        if pending.get("phase") in {"WAIT_CANCEL", "READY"}:
            from sigbot.strategies.grid_execution import replacement_state

            phase = replacement_state(
                session.get(Order, pending["source_order_id"]), pending, self.clock.now
            )
            event.data = {**event.data, "entry_replacement": {**pending, "phase": phase}}
            if phase in {"WAIT_CANCEL", "READY"}:
                # Keep the original direction/plan and active-book priority
                # across a zero-fill cancel, including process recovery.
                event.state = "ENTRY_PENDING"
        if (
            not was_closed
            and event.state == "CLOSED"
            and event.data.get("plan")
            and meta.get("used_slots")
        ):
            event.data = {
                **event.data,
                "rearm": {
                    **event.data.get("rearm", {}),
                    inst.outcome: {
                        "reference": event.data["plan"]["target"],
                        "recovered": False,
                        "last": None,
                    },
                },
            }
