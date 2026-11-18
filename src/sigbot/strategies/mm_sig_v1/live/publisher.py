"""MM proposals to the durable shared allocator; no order API or executor ownership."""

import json
from datetime import datetime, timedelta, timezone
from decimal import ROUND_FLOOR, Decimal

from sqlalchemy import or_, select

from sigbot.command_queue import CommandQueue
from sigbot.config import Config
from sigbot.domain import TERMINAL, ZERO, Intent, content_hash, utc_time
from sigbot.storage import ExecutionCommand, Instrument, IntentRow, Order, Position, StrategyEvent
from sigbot.strategies.mm_sig_v1.live.inputs import actual_inventory


def now():
    return datetime.now(timezone.utc)


class Publisher:
    def __init__(self, runtime):
        self.runtime = runtime
        self.app = Config.load(runtime.settings.application_config)
        if (self.app.mm_risk_policy is None) != (runtime.settings.risk_policy is None) or (
            self.app.mm_risk_policy is not None and self.app.mm_risk_policy.digest != runtime.settings.risk_policy.digest
        ):
            raise ValueError("MM_PRODUCER_RISK_POLICY_MISMATCH")
        self.queue = CommandQueue(
            runtime.live_store, runtime.aid, "mm_sig", runtime.settings.strategy_version
        )
        self.published = 0
        if runtime.settings.approval_file:
            from pathlib import Path

            authority = json.loads(Path(runtime.settings.approval_file).read_text())
            runtime.state["empirical_fee_accepted_by_operator"] = bool(
                authority.get("operator_approved")
                and authority.get("accept_empirical_fee_model")
                and now() < utc_time(authority["new_orders_until"])
            )

    def own_orders(self, session, instrument=None):
        result = []
        query = (
            select(Order, IntentRow)
            .join(IntentRow, Order.intent_id == IntentRow.id)
            .where(
                Order.account_id == self.runtime.aid,
                or_(
                    Order.state.not_in(TERMINAL),
                    Order.request_state != "RESOLVED",
                    Order.reserve_cash > ZERO,
                    Order.reserve_quantity > ZERO,
                ),
            )
        )
        if instrument:
            query = query.where(Order.instrument_id == instrument)
        query = query.where(IntentRow.payload["strategy_id"].as_string() == "mm_sig")
        for order, row in session.execute(query):
            if self.runtime.settings.strategy_version == "1.3":
                from sigbot.strategies.mm_sig_v1.live.analytics import owned

                if not owned(row):
                    continue
            elif (
                row.payload["strategy_id"],
                row.payload["strategy_version"],
            ) != self.queue.identity:
                continue
            if instrument and order.instrument_id != instrument:
                continue
            if (
                order.state not in TERMINAL
                or order.request_state != "RESOLVED"
                or order.reserve_cash
                or order.reserve_quantity
            ):
                result.append(order)
        return result

    def maintain(self):
        if self.runtime.settings.strategy_version == "1.3":
            timestamp = now()
            with self.runtime.live_store.transaction() as session:
                working = {o.instrument_id for o in self.own_orders(session)}
                working.update(
                    session.scalars(
                        select(ExecutionCommand.instrument_id).where(
                            ExecutionCommand.account_id == self.runtime.aid,
                            ExecutionCommand.strategy_id == "mm_sig",
                            ExecutionCommand.action == "SUBMIT",
                            ExecutionCommand.state.in_(["WAITING_ALLOCATION", "OMS_PENDING"]),
                        )
                    )
                )
            keys = list(getattr(self.runtime, "tracking_exchanges", []))
            if self.runtime.settings.complementary_quotes_enabled:
                keys += [eid + ":NO" for eid in keys]
            for eid in keys:
                quote = self.runtime.state.get("quotes", {}).get(eid)
                refs = self.runtime.state.get("reference_sampler", {})
                observed = refs.get("exchanges", {}).get(eid, {})
                review = dict(self.runtime.reviews.get(eid.split(":")[0], {}))
                if eid.endswith(":NO") and review:
                    review["instrument_id"] = review["instrument_id"].removesuffix("-YES") + "-NO"
                if review.get("instrument_id") not in working:
                    continue
                reference_current = bool(
                    refs.get("at")
                    and 0
                    <= (timestamp - utc_time(refs["at"])).total_seconds()
                    <= self.runtime.settings.book_ttl_seconds
                    and observed.get("current")
                    and observed.get("rules_hash") == review.get("rules_hash")
                    and observed.get("mapping_hash") == review.get("mapping_hash")
                    and quote
                    and not (
                        set(quote.get("blocking_reasons", []))
                        - {"MM13_TWO_CONSISTENT_CHECKS_REQUIRED"}
                    )
                    and quote.get("rules_hash") == review.get("rules_hash")
                    and quote.get("mapping_hash") == review.get("mapping_hash")
                    and not self.runtime.state.get("reference_sampler_error")
                )
                if (
                    quote is None
                    or (timestamp - utc_time(quote["validated_at"])).total_seconds()
                    > self.runtime.settings.book_ttl_seconds
                ) and not reference_current:
                    if review:
                        self.suspend(review["instrument_id"], "MM13_DATA_UNCERTAIN")
            return
        with self.runtime.live_store.transaction() as s:
            orders = self.own_orders(s)
            expired = [
                o.id
                for o in orders
                if self.runtime.settings.resting_order_seconds is not None
                and now() >= utc_time(s.get(IntentRow, o.intent_id).payload["expires_at"])
            ]
        for oid in expired:
            self.queue.cancel(oid, now())

    def publish(self, review, prices, book):
        if self.runtime.settings.strategy_version == "1.3":
            return self.publish13(review, prices, book)
        if prices["blocking_reasons"]:
            return
        r, settings = self.runtime, self.runtime.settings
        iid = review["instrument_id"]
        with r.live_store.transaction() as s:
            inst = s.get(Instrument, iid)
            own = self.own_orders(s, iid)
            held = actual_inventory(r.account_data, review["exchange_id"])
            event = s.get(
                StrategyEvent, (r.aid, "mm_sig", settings.strategy_version, inst.event_group)
            )
            seed = (
                settings.bootstrap_inventory
                and not held
                and not own
                and not (event and event.data.get("seed_attempted"))
            )
            pending_buy = sum((o.quantity - o.filled for o in own if o.side == "BUY"), ZERO)
            pending_sell = sum((o.reserve_quantity for o in own if o.side == "SELL"), ZERO)
        if seed:
            ask = Decimal(prices["external_ask"])
            depth = sum(
                (level.quantity - level.own_quantity for level in book.asks if level.price == ask),
                ZERO,
            )
            proposals = [
                (
                    "BUY",
                    min(Decimal(prices["buy_quantity"]), depth).to_integral_value(
                        rounding=ROUND_FLOOR
                    ),
                    ask,
                    "MM_REAL_SEED",
                )
            ]
        else:
            proposals = [
                ("BUY", Decimal(prices["buy_quantity"]), Decimal(prices["bid"]), "MM_REAL_PASSIVE"),
                (
                    "SELL",
                    Decimal(prices["sell_quantity"]),
                    Decimal(prices["ask"]),
                    "MM_REAL_PASSIVE",
                ),
            ]
        for side, qty, price, kind in proposals:
            if qty <= 0:
                continue
            active = [o for o in own if o.side == side]
            if active:
                if all(o.price == price for o in active):
                    continue
                for order in active:
                    self.queue.cancel(order.id, now())
                continue  # Confirmed closure/late fills precede replacing a quote.
            timestamp = now()
            target = Intent.model_validate(
                {
                    "schema_version": "1.0",
                    "intent_id": "SIG-MM-"
                    + content_hash([r.aid, iid, side, kind, timestamp.isoformat()]),
                    "synthetic": False,
                    "account_id": r.aid,
                    "tournament_id": r.tid,
                    "instrument_id": iid,
                    "outcome_side": review["instrument_id"].rsplit("-", 1)[-1],
                    "action": "INCREASE_TO_TARGET" if side == "BUY" else "REDUCE_TO_TARGET",
                    "target_quantity": held + pending_buy + qty
                    if side == "BUY"
                    else max(ZERO, held - pending_sell - qty),
                    "limit_price": price,
                    "max_order_notional": self.app.risk.max_order_notional
                    or qty * price + qty * inst.fee_per_share,
                    "created_at": timestamp,
                    "expires_at": timestamp
                    + timedelta(seconds=settings.resting_order_seconds or 300),
                    "rules_hash": review["rules_hash"],
                    "mapping_hash": review["mapping_hash"],
                    "strategy_id": "mm_sig",
                    "strategy_version": settings.strategy_version,
                    "note": json.dumps(
                        {"kind": kind, "settings": settings.model_dump(mode="json")}
                    ),
                }
            )
            self.queue.publish(target)
            self.published += 1
        r.state.update(
            execution_role="PRODUCER_ONLY",
            queued_proposals=self.published,
            shared_executor="sigbot-account-executor.service",
            trading_enabled=False,
        )

    def cancel_owned(self, orders, reason="MM_SAFETY_WITHDRAWAL", *, safety=True):
        for order in orders:
            with self.runtime.live_store.transaction() as session:
                row = session.get(IntentRow, order.intent_id)
                version = row.payload["strategy_version"]
            CommandQueue(self.runtime.live_store, self.runtime.aid, "mm_sig", version).cancel(
                order.id, now(), reason=reason, safety=safety
            )

    def suspend(self, instrument_id, reason):
        with self.runtime.live_store.transaction() as session:
            orders = self.own_orders(session, instrument_id)
            for command in session.scalars(
                select(ExecutionCommand).where(
                    ExecutionCommand.account_id == self.runtime.aid,
                    ExecutionCommand.strategy_id == "mm_sig",
                    ExecutionCommand.instrument_id == instrument_id,
                    ExecutionCommand.action == "SUBMIT",
                    ExecutionCommand.state == "WAITING_ALLOCATION",
                )
            ):
                command.state, command.result = "SUPERSEDED_BEFORE_ALLOCATION", {"code": reason}
        self.cancel_owned(orders, reason)
        self.runtime.state["last_safety_action"] = {
            "instrument_id": instrument_id,
            "reason": reason,
            "cancels_queued": len(orders),
            "reserves_released": False,
        }

    def publish13(self, review, quote, book):
        from sigbot.strategies.mm_sig_v1.live.lifecycle import keep_order

        r, settings = self.runtime, self.runtime.settings
        iid, timestamp = review["instrument_id"], now()
        if quote["blocking_reasons"]:
            self.suspend(iid, "MM13_DATA_UNCERTAIN")
            return
        with r.live_store.transaction() as session:
            own = self.own_orders(session, iid)
            pos = session.get(Position, (r.aid, iid))
            held = pos.quantity if pos else ZERO
            rows = {o.id: session.get(IntentRow, o.intent_id) for o in own}
            pending = [
                c
                for c in session.scalars(
                    select(ExecutionCommand).where(
                        ExecutionCommand.account_id == r.aid,
                        ExecutionCommand.strategy_id == "mm_sig",
                        ExecutionCommand.instrument_id == iid,
                        ExecutionCommand.action == "SUBMIT",
                        ExecutionCommand.state.in_(["WAITING_ALLOCATION", "OMS_PENDING"]),
                    )
                )
            ]
        proposals = [
            (
                "BUY",
                Decimal(quote["buy_quantity"]),
                Decimal(quote["bid"]) if quote["bid"] is not None else None,
                "MM_REAL_PASSIVE",
            ),
            (
                "SELL",
                Decimal(quote["sell_quantity"]),
                Decimal(quote["ask"]) if quote["ask"] is not None else None,
                quote["sell_kind"],
            ),
        ]
        for side, total, price, kind in proposals:
            active = [o for o in own if o.side == side]
            if active:
                if len(active) != 1 or not keep_order(
                    active[0], rows[active[0].id], price, total, quote, settings, timestamp
                ):
                    directive = quote.get("buy_directive" if side == "BUY" else "sell_directive")
                    reason = quote.get(
                        "buy_directive_reason" if side == "BUY" else "sell_directive_reason",
                        "MM_NORMAL_REPRICE",
                    )
                    safety = directive == "WITHDRAW" or any(
                        o.quantity - o.filled
                        > Decimal(
                            quote["maximum_buy_total" if side == "BUY" else "maximum_sell_total"]
                        )
                        or (
                            o.price > Decimal(quote["buy_guard"])
                            if side == "BUY"
                            else o.price
                            < Decimal(
                                quote["ask"]
                                if quote["sell_kind"] == "INVENTORY_EXIT"
                                else quote["sell_guard"]
                            )
                        )
                        for o in active
                    )
                    self.cancel_owned(
                        active, reason if safety else "MM_NORMAL_REPRICE", safety=safety
                    )
                continue
            if (
                total <= 0
                or price is None
                or quote.get("buy_directive" if side == "BUY" else "sell_directive") == "DEFER"
                or not quote.get("allow_new_orders", True)
            ):
                continue
            action = "INCREASE_TO_TARGET" if side == "BUY" else "REDUCE_TO_TARGET"
            if any(c.payload["action"] == action and timestamp < c.expires_at for c in pending):
                continue
            intent = Intent.model_validate(
                {
                    "schema_version": "1.0",
                    "intent_id": "SIG-MM-"
                    + content_hash([r.aid, iid, side, kind, timestamp.isoformat()]),
                    "synthetic": False,
                    "account_id": r.aid,
                    "tournament_id": r.tid,
                    "instrument_id": iid,
                    "outcome_side": review["instrument_id"].rsplit("-", 1)[-1],
                    "action": action,
                    "target_quantity": held + total if side == "BUY" else max(ZERO, held - total),
                    "limit_price": price,
                    "max_order_notional": self.app.risk.max_order_notional
                    or total * price + total * (r.fee or ZERO),
                    "created_at": timestamp,
                    "expires_at": timestamp + timedelta(seconds=300),
                    "rules_hash": review["rules_hash"],
                    "mapping_hash": review["mapping_hash"],
                    "strategy_id": "mm_sig",
                    "strategy_version": "1.3",
                    "note": json.dumps(
                        {
                            "kind": kind,
                            "settings": settings.model_dump(mode="json"),
                            "policy": quote,
                        }
                    ),
                }
            )
            self.queue.publish(intent)
            self.published += 1
        r.state.update(
            execution_role="PRODUCER_ONLY", queued_proposals=self.published, trading_enabled=False
        )
