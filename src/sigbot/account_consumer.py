"""One OMS owner allocates durable proposals from registered strategies."""

import json
from contextlib import nullcontext
from datetime import timedelta

from sqlalchemy import select

from sigbot.domain import TERMINAL, Intent, content_hash, utc_time
from sigbot.errors import AuthorizationRequired, Conflict, SigbotError
from sigbot.storage import ExecutionCommand, IntentRow, Order


class AccountConsumer:
    def __init__(self, executor, activate=None, refresh_book=None, refresh_reference=None):
        self.executor, self.store, self.clock = executor, executor.store, executor.clock
        self.activate = activate or (lambda identity, action: nullcontext())
        self.refresh_book = refresh_book
        self.refresh_reference = refresh_reference
        self.last_family = None
        self.last_instrument = None

    def process(self, command_id):
        e = self.executor
        e.ownership()
        with self.store.transaction() as s:
            command = s.get(ExecutionCommand, command_id)
            if command is None or command.account_id != e.account_id:
                raise AuthorizationRequired("SHARED_COMMAND_ACCOUNT_MISMATCH")
            identity = command.strategy_id, command.strategy_version
            if command.state not in {"WAITING_ALLOCATION", "OMS_PENDING"}:
                return dict(command.result)
            if (command.strategy_id, command.strategy_version) == (
                "ssr_sig",
                "1.0",
            ) and command.result.get("retry_at"):
                order = s.get(Order, command.order_id) if command.order_id else None
                if (
                    order
                    and order.request_state == "SEND_PENDING"
                    and self.clock.now < utc_time(command.result["retry_at"])
                ):
                    return dict(command.result)
            if command.digest != content_hash(command.payload):
                raise Conflict("SHARED_COMMAND_DIGEST_CHANGED")
            action, payload, existing_order = (
                command.action,
                dict(command.payload),
                command.order_id,
            )
            if (
                command.state == "WAITING_ALLOCATION"
                and self.clock.now >= utc_time(command.expires_at)
                and not (command.action == "CANCEL" and command.payload.get("risk_latched"))
            ):
                command.state, command.result = (
                    "EXPIRED_BEFORE_ALLOCATION",
                    {"code": "COMMAND_EXPIRED"},
                )
                return dict(command.result)
        try:
            with self.activate(identity, "READ" if existing_order else action):
                if existing_order:
                    if identity == ("ssr_sig", "1.0") and action == "SUBMIT" and self.refresh_book:
                        with self.store.transaction() as s:
                            order = s.get(Order, existing_order)
                            row = s.get(IntentRow, order.intent_id)
                            needs_quote = (
                                order.request_state == "SEND_PENDING"
                                and self.clock.now < utc_time(row.payload["expires_at"])
                            )
                            instrument_id = order.instrument_id
                        if needs_quote:
                            self.refresh_book(instrument_id)
                    self._continue(existing_order, action, command_id)
                elif action == "SUBMIT":
                    intent = Intent.model_validate(payload)
                    if (intent.account_id, intent.strategy_id, intent.strategy_version) != (
                        e.account_id,
                        *identity,
                    ):
                        raise AuthorizationRequired("SHARED_INTENT_SCOPE_MISMATCH")
                    if self.refresh_book:
                        self.refresh_book(intent.instrument_id)
                        if identity == ("mm_sig", "1.3"):
                            self.refresh_book(intent.instrument_id)
                    if (
                        identity == ("ecr_sig", "1.1")
                        and self.refresh_reference
                        and json.loads(intent.note or "{}").get("kind") == "ENTRY"
                    ):
                        self.refresh_reference(intent.instrument_id)
                    if identity == ("mm_sig", "1.3"):
                        with self.store.transaction() as session:
                            account = self.store.lock_account(session, e.account_id)
                            intent = e.planner.policy(intent).rebuild_intent(
                                session, account, intent
                            )
                    if identity in {("grid_sig", "1.0"), ("grid_sig", "2.0"), ("ecr_sig", "1.1")}:
                        with self.store.transaction() as session:
                            account = self.store.lock_account(session, e.account_id)
                            intent = e.planner.policy(intent).rebuild_intent(
                                session, account, intent
                            )
                    if identity == ("mm_sig", "1.2"):
                        from decimal import Decimal

                        from sigbot.storage import Instrument, Position
                        from sigbot.strategies.mm_sig_v1.live.execution import account_capital
                        from sigbot.strategies.mm_sig_v1.live.inputs import actual_prices
                        from sigbot.strategies.mm_sig_v1.live.models import Settings
                        from sigbot.strategies.mm_sig_v1.models import Book

                        note = json.loads(intent.note or "{}")
                        policy = Settings.model_validate(note["settings"])
                        with self.store.transaction() as session:
                            account = self.store.lock_account(session, e.account_id)
                            instrument = session.get(Instrument, intent.instrument_id)
                            position = session.get(Position, (e.account_id, intent.instrument_id))
                            held = position.quantity if position else Decimal(0)
                            book = Book.model_validate(
                                {
                                    **instrument.book,
                                    "received_at": instrument.book_at,
                                    "quality": instrument.quality,
                                }
                            )
                            quote = actual_prices(
                                book,
                                held,
                                instrument.tick,
                                policy,
                                fee=instrument.fee_per_share,
                                capital=account_capital(session, account),
                            )
                            side = "BUY" if intent.action == "INCREASE_TO_TARGET" else "SELL"
                            quantity = Decimal(
                                quote["buy_quantity" if side == "BUY" else "sell_quantity"]
                            )
                            if note["kind"] == "MM_REAL_SEED":
                                price = Decimal(quote["external_ask"])
                                depth = sum(
                                    (
                                        level.quantity - level.own_quantity
                                        for level in book.asks
                                        if level.price == price
                                    ),
                                    Decimal(0),
                                )
                                quantity = min(quantity, depth).to_integral_value(
                                    rounding="ROUND_FLOOR"
                                )
                            else:
                                price = Decimal(quote["bid" if side == "BUY" else "ask"])
                        intent = intent.model_copy(
                            update={
                                "target_quantity": held + quantity
                                if side == "BUY"
                                else max(Decimal(0), held - quantity),
                                "limit_price": price,
                                "created_at": self.clock.now,
                                "expires_at": self.clock.now + timedelta(seconds=30),
                            }
                        )
                    # Allocation and binding to its durable command are atomic.
                    with self.store.transaction() as s:
                        account = self.store.lock_account(s, e.account_id)
                        command = s.get(ExecutionCommand, command_id)
                        if command.state != "WAITING_ALLOCATION":
                            return dict(command.result)
                        order_id = e.planner.reserve_locked(s, account, intent)
                        command.order_id = order_id
                        command.state = "OMS_PENDING" if order_id else "NO_ORDER_REQUIRED"
                        command.result = {"order_id": order_id, "allocated": True}
                    if order_id:
                        self._continue(order_id, action)
                elif action == "CANCEL":
                    with self.store.transaction() as s:
                        account = self.store.lock_account(s, e.account_id)
                        order = s.get(Order, payload["order_id"])
                        owner = s.get(IntentRow, order.intent_id) if order else None
                        if (
                            order is None
                            or owner is None
                            or order.account_id != account.id
                            or (owner.payload["strategy_id"], owner.payload["strategy_version"])
                            != identity
                        ):
                            raise AuthorizationRequired("SHARED_CANCEL_OWNER_MISMATCH")
                        command = s.get(ExecutionCommand, command_id)
                        command.order_id, command.state = order.id, "OMS_PENDING"
                        command.result = {
                            **command.result,
                            "claimed_at": self.clock.now.isoformat(),
                        }
                    self._continue(payload["order_id"], action, command_id)
                else:
                    raise AuthorizationRequired("SHARED_COMMAND_ACTION_UNSUPPORTED")
        except SigbotError as exc:
            with self.store.transaction() as s:
                command = s.get(ExecutionCommand, command_id)
                command.result = {**command.result, "code": exc.code}
                if command.order_id is None:
                    command.state = "REJECTED_BEFORE_ALLOCATION"
                else:
                    order = s.get(Order, command.order_id)
                    # Only a proven unsent request may be abandoned locally.
                    if order.request_state == "SEND_PENDING" and not order.remote_id:
                        command.result = {**command.result, "unsent": True}
                        if identity == ("ssr_sig", "1.0"):
                            delay = 60 if "BUDGET" in exc.code or "RATE" in exc.code else 15
                            command.result = {
                                **command.result,
                                "retry_at": (self.clock.now + timedelta(seconds=delay)).isoformat(),
                            }
                self.store.audit(
                    s, e.account_id, "SHARED_COMMAND_BLOCKED", command.result, self.clock.now
                )
        self.last_family = identity
        self.last_instrument = command.instrument_id
        return self.finish(command_id)

    def _continue(self, order_id, action, command_id=None):
        with self.store.transaction() as s:
            order = s.get(Order, order_id)
            terminal = order.state in TERMINAL and order.request_state == "RESOLVED"
            request_state, cancel_state = order.request_state, order.cancel_state
        if terminal:
            return
        if action == "CANCEL":
            if cancel_state in {"CANCEL_REQUESTED", "CANCEL_UNKNOWN"} or request_state in {
                "UNKNOWN",
                "SENDING",
            }:
                self.executor.resolve(order_id)
                if cancel_state in {"CANCEL_REQUESTED", "CANCEL_UNKNOWN"}:
                    self.executor.retry_pending_cancel(order_id)
            else:
                if command_id and self.refresh_book and request_state == "ACKNOWLEDGED":
                    from sigbot.strategies.mm_sig_v1.live.lifecycle import RECOVERABLE_DATA_CANCELS

                    with self.store.transaction() as s:
                        command = s.get(ExecutionCommand, command_id)
                        recoverable = (
                            command.strategy_id == "mm_sig"
                            and command.strategy_version == "1.3"
                            and command.payload.get("reason") in RECOVERABLE_DATA_CANCELS
                        )
                        instrument_id = s.get(Order, order_id).instrument_id
                    if recoverable:
                        try:
                            # Two real REST proofs, never a retimestamped old recommendation.
                            self.refresh_book(instrument_id)
                            self.refresh_book(instrument_id)
                        except SigbotError as exc:
                            with self.store.transaction() as s:
                                self.store.audit(
                                    s,
                                    self.executor.account_id,
                                    "MM_CANCEL_REVALIDATION_READ_FAILED",
                                    {"order": order_id, "code": exc.code},
                                    self.clock.sync(),
                                )
                            # Preserve the original safety cancellation on failed recovery.

                def before_send(session, order):
                    from sigbot.strategies.mm_sig_v1.live.lifecycle import revalidate_cancel

                    command = session.get(ExecutionCommand, command_id)
                    return revalidate_cancel(session, order, command, self.clock.sync())

                self.executor.cancel(order_id, before_send=before_send if command_id else None)
        elif request_state == "SEND_PENDING":
            with self.store.transaction() as s:
                order = s.get(Order, order_id)
                row = s.get(IntentRow, order.intent_id)
                expired = self.clock.now >= utc_time(row.payload["expires_at"])
            if expired:
                self.executor.cancel(order_id)  # Proven unsent; release via the normal OMS.
            else:
                self.executor.dispatch(order_id)
        else:
            self.executor.resolve(order_id)  # ACKNOWLEDGED/UNKNOWN are never new POSTs.

    def finish(self, command_id):
        with self.store.transaction() as s:
            command = s.get(ExecutionCommand, command_id)
            if command.order_id:
                order = s.get(Order, command.order_id)
                command.result = {
                    **command.result,
                    "order_id": order.id,
                    "order_state": order.state,
                    "request_state": order.request_state,
                    "cancel_state": order.cancel_state,
                    "filled": str(order.filled),
                }
                if order.state in TERMINAL and order.request_state == "RESOLVED":
                    command.state = "OMS_CLOSED"
            return {"command_state": command.state, **command.result}

    def pump(self, limit=4):
        self.executor.ownership()
        with self.store.transaction() as s:
            commands = list(
                s.scalars(
                    select(ExecutionCommand)
                    .where(
                        ExecutionCommand.account_id == self.executor.account_id,
                        ExecutionCommand.state == "WAITING_ALLOCATION",
                    )
                    .order_by(ExecutionCommand.created_at)
                )
            )
            # Expired, never-allocated signals have no OMS/risk side effects.
            # Sweep them in one local transaction so backlog cannot starve exits.
            current = []
            for command in commands:
                if (command.order_id is None and self.clock.now >= utc_time(command.expires_at)
                        and not (command.action == "CANCEL" and command.payload.get("risk_latched"))):
                    command.state = "EXPIRED_BEFORE_ALLOCATION"
                    command.result = {"code": "COMMAND_EXPIRED"}
                else:
                    current.append(command)
            commands = current
        commands.sort(
            key=lambda c: (
                c.action != "CANCEL",
                c.action == "SUBMIT" and c.payload.get("action") != "REDUCE_TO_TARGET",
                -int(
                    json.loads(c.payload.get("note") or "{}")
                    .get("policy", {})
                    .get("exit_priority", 0)
                )
                if c.action == "SUBMIT"
                and c.strategy_id == "mm_sig"
                and c.strategy_version == "1.3"
                else 0,
                c.action == "SUBMIT"
                and c.strategy_id == "mm_sig"
                and c.payload.get("action") != "REDUCE_TO_TARGET",
                self.last_family == (c.strategy_id, c.strategy_version),
                c.instrument_id == self.last_instrument if c.action == "SUBMIT" else False,
                c.created_at,
            )
        )
        return [self.process(c.id) for c in commands[:limit]]

    def pending(self, *, acknowledged_via_snapshot=False, after_command=None):
        with self.store.transaction() as s:
            commands = list(
                s.scalars(
                    select(ExecutionCommand).where(
                        ExecutionCommand.account_id == self.executor.account_id,
                        ExecutionCommand.state == "OMS_PENDING",
                    )
                )
            )
            deferred = set()
            if acknowledged_via_snapshot:
                for command in commands:
                    order = s.get(Order, command.order_id) if command.order_id else None
                    if (
                        command.action == "SUBMIT"
                        and order is not None
                        and order.request_state == "ACKNOWLEDGED"
                        and order.state in {"OPEN", "PARTIAL"}
                        and not order.cancel_state
                    ):
                        deferred.add(command.id)
        # The shared runtime's complete account snapshot already verifies every
        # acknowledged order and fills. Unknown and cancel recovery stay eager.
        results = []
        for command in commands:
            results.append(
                self.finish(command.id) if command.id in deferred else self.process(command.id)
            )
            if after_command is not None:
                after_command()
        return results
