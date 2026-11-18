from __future__ import annotations

import hashlib
import json
from contextlib import contextmanager
from dataclasses import asdict
from datetime import timedelta
from decimal import Decimal

from sqlalchemy import select, text
from sqlalchemy.exc import OperationalError, SQLAlchemyError

from sigbot.accounting import Ledger
from sigbot.adapters import Exchange
from sigbot.config import Config
from sigbot.domain import (
    TERMINAL,
    ZERO,
    Clock,
    Intent,
    Mode,
    Observation,
    OrderRequest,
    content_hash,
    merge_state,
    utc_time,
)
from sigbot.errors import (
    AuthorizationRequired,
    Conflict,
    ContractNotVerified,
    OrderRejected,
    RateLimited,
    RiskRejected,
    SchemaMismatch,
    StorageUnavailable,
    SubmissionNotSent,
    TransportUnknown,
)
from sigbot.operations import Controls, Leader
from sigbot.risk import Planner
from sigbot.scheduler import Scheduler
from sigbot.storage import Audit, Inbox, Instrument, IntentRow, Order, Outbox, RequestAttempt, Store


class Executor:
    @staticmethod
    def probe_once(key: str, probe_id: str, **kwargs) -> dict:
        """Explicit single-order diagnostic; normal dispatch remains fail-closed."""
        from sigbot.orderprobe import ProbeExecutor

        return ProbeExecutor(key, probe_id, **kwargs).run()

    def __init__(
        self,
        store: Store,
        config: Config,
        clock: Clock,
        exchange: Exchange,
        planner: Planner,
        account_id: str,
    ):
        self.store, self.config, self.clock, self.exchange = store, config, clock, exchange
        self.planner, self.account_id = planner, account_id
        self.controls, self.ledger = Controls(store, clock), Ledger(store)
        self.scheduler = Scheduler(store, config, clock)
        self.leader = Leader(store, account_id)

    def start(self, restored_backup: bool = False) -> None:
        self.leader.acquire(shared=self.config.parallel_strategy_execution)
        try:
            if not self.exchange.synthetic:
                from sigbot.live import SigExecutionExchange

                if isinstance(self.exchange, SigExecutionExchange):
                    self.exchange.acquire_write_lock()
            if not self.config.parallel_strategy_execution or restored_backup:
                with self.account_operation(wait_seconds=60):
                    self.controls.recover(self.account_id, restored_backup)
        except (SQLAlchemyError, Conflict, RateLimited):
            self.close()
            raise

    @contextmanager
    def account_operation(self, *, wait_seconds=5, order_id=None):
        """Parallel producers; only an in-flight account operation is exclusive."""
        if not self.config.parallel_strategy_execution:
            yield
            return
        key = int.from_bytes(
            hashlib.sha256(
                (
                    ("sig-order-operation:" + order_id)
                    if order_id
                    else ("sig-account-operation:" + self.account_id)
                ).encode()
            ).digest()[:8],
            "big",
            signed=True,
        )
        with self.store.engine.connect() as connection, connection.begin():
            connection.execute(text("SET LOCAL lock_timeout = '" + str(int(wait_seconds)) + "s'"))
            try:
                connection.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": key})
            except OperationalError as exc:
                if getattr(exc.orig, "sqlstate", None) == "55P03":
                    raise RateLimited("ACCOUNT_OPERATION_BUSY") from None
                raise StorageUnavailable("ACCOUNT_OPERATION_STORAGE_UNAVAILABLE") from None
            yield

    def projection_signature(self):
        from sigbot.storage import Position

        with self.store.transaction() as session:
            from sigbot.storage import Account

            account = session.get(Account, self.account_id)
            assert account is not None
            positions = sorted(
                (p.instrument_id, str(p.quantity), str(p.cost))
                for p in session.scalars(
                    select(Position).where(Position.account_id == self.account_id)
                )
            )
            orders = sorted(
                (o.id, o.request_state, str(o.filled), o.cancel_state, o.remote_id)
                for o in session.scalars(select(Order).where(Order.account_id == self.account_id))
                if o.remote_id is not None or o.request_state in {"SENDING", "UNKNOWN"}
            )
            return content_hash([str(account.cash), positions, orders])

    def reconcile(self):
        from sigbot.reconciliation import Reconciler

        with self.account_operation(wait_seconds=60):
            before = (
                self.projection_signature() if self.config.parallel_strategy_execution else None
            )
            snapshot = self.exchange.snapshot()
            if before is not None and before != self.projection_signature():
                self.controls.pause(self.account_id, "SNAPSHOT_WINDOW_INDETERMINATE")
                return {
                    "status": "INDETERMINATE",
                    "differences": ["LOCAL_PROJECTION_CHANGED_DURING_REAL_SNAPSHOT"],
                    "checkpoint": snapshot.checkpoint,
                    "layer": "REAL_READ",
                }
            return Reconciler(self).run(snapshot)

    def close(self) -> None:
        self.leader.close()
        if not self.exchange.synthetic:
            from sigbot.live import SigExecutionExchange

            if isinstance(self.exchange, SigExecutionExchange):
                self.exchange.release_write_lock()

    def synthetic_only(self) -> None:
        if not self.exchange.synthetic or self.config.mode not in {Mode.OFFLINE, Mode.SHADOW}:
            raise AuthorizationRequired("LIVE_WRITES_DISABLED_IN_THIS_DELIVERY")

    def execution_permission(self) -> None:
        if self.exchange.synthetic:
            self.synthetic_only()
            return
        from sigbot.live import SigExecutionExchange

        if not isinstance(self.exchange, SigExecutionExchange):
            raise AuthorizationRequired("OFFICIAL_EXECUTION_BRIDGE_REQUIRED")
        if self.config.mode not in {Mode.LIVE_CANARY, Mode.LIVE_LIMITED}:
            raise AuthorizationRequired("LIVE_CONFIGURATION_REQUIRED")
        self.exchange.authorized()

    def ownership(self) -> None:
        try:
            self.leader.check()
            if not self.exchange.synthetic:
                from sigbot.live import SigExecutionExchange

                assert isinstance(self.exchange, SigExecutionExchange)
                self.exchange.check_write_lock()
        except StorageUnavailable:
            self.controls.pause(self.account_id, "EXECUTOR_LOCK_LOST")
            raise

    def dispatch(self, order_id: str, fault=None) -> Observation | None:
        with self.account_operation(order_id=order_id):
            return self._dispatch(order_id, fault)

    def _dispatch(self, order_id: str, fault=None) -> Observation | None:
        self.execution_permission()
        self.ownership()
        if not self.exchange.synthetic:
            from sigbot.live import SigExecutionExchange

            assert isinstance(self.exchange, SigExecutionExchange)
            with self.store.transaction() as s:
                account = self.store.lock_account(s, self.account_id)
                pending = s.get(Order, order_id)
                if pending is None or pending.account_id != self.account_id:
                    raise Conflict("ORDER_SCOPE_MISMATCH")
                if pending.request_state != "SEND_PENDING":
                    return None
                self.exchange.approve(s, account, pending, "SUBMIT")
                instrument_id = pending.instrument_id
                inst = s.get(Instrument, instrument_id)
                assert inst is not None
                checked = inst.mapping_evidence.get("live_checked_at")
                needs_refresh = checked is None or not 0 <= (
                    self.clock.now - utc_time(checked)
                ).total_seconds() <= min(5, self.config.book_ttl_seconds)
            if needs_refresh:
                self.exchange.refresh_book(instrument_id)
        with self.store.transaction() as s:
            account = self.store.lock_account(s, self.account_id)
            order = s.get(Order, order_id)
            if order is None or order.account_id != self.account_id:
                raise Conflict("ORDER_SCOPE_MISMATCH")
            if order.request_state != "SEND_PENDING":
                return None  # UNKNOWN/SENDING are never automatically retried.
            row = s.get(IntentRow, order.intent_id)
            assert row is not None
            intent = Intent.model_validate(row.payload)
            if self.clock.now >= intent.expires_at or self.clock.now >= self.planner.ends_at:
                order.request_state, order.state = "RESOLVED", "EXPIRED"
                order.reserve_cash, order.reserve_quantity, row.active = ZERO, ZERO, False
                out = s.get(Outbox, order.id)
                assert out is not None
                out.state = "EXPIRED_UNSENT"
                self.planner.on_unsent_expired(s, account, intent, order)
                return None
            inst = s.scalar(
                select(Instrument).where(Instrument.id == order.instrument_id).with_for_update()
            )
            assert inst is not None
            if account.lifecycle != "HEALTHY" or account.reconciled_at is None:
                raise RiskRejected("RECONCILIATION_REQUIRED_OR_PAUSED")
            if (
                self.clock.now - utc_time(account.reconciled_at)
            ).total_seconds() > self.config.account_ttl_seconds:
                raise RiskRejected("RECONCILIATION_REQUIRED")
            if (
                self.clock.now < self.planner.starts_at
                or self.clock.now
                >= self.planner.ends_at - timedelta(seconds=0 if intent.strategy_id == "mm_sig"
                    and self.config.mm_risk_policy is not None and order.side == "SELL"
                    else self.config.drain_seconds)
            ):
                raise RiskRejected("TRADING_CLOSED")
            if (
                not inst.reviewed
                or inst.review_until is None
                or utc_time(inst.review_until) <= self.clock.now
            ):
                raise RiskRejected("MARKET_NOT_REVIEWED")
            if (inst.rules_hash, inst.mapping_hash) != (intent.rules_hash, intent.mapping_hash):
                self.store.audit(s,account.id,"DOCUMENT_BINDING_DIFFERENCE",
                    {"policy":"REPORT_ONLY","intent":intent.intent_id,"old":[intent.rules_hash,intent.mapping_hash],
                     "new":[inst.rules_hash,inst.mapping_hash]},self.clock.now)
            data_at = self.planner.market_data_time(inst, intent)
            self.clock.sync()
            if (
                inst.status != "OPEN"
                or inst.quality != "HEALTHY"
                or inst.book_at is None
                or data_at is None
                or not 0
                <= (self.clock.now - utc_time(data_at)).total_seconds()
                <= self.config.book_ttl_seconds
            ):
                raise RiskRejected("DATA_NOT_READY")
            self.planner.check_liquidity(inst, order.side, order.price, order.quantity, intent)
            from sigbot.uncertainty import check_scope

            check_scope(s, account, inst, self.config.parallel_strategy_execution)
            self.planner.check_capital(
                s, account, intent, order.side, order.reserve_cash, exclude_order=order.id
            )
            if intent.strategy_id == "mm_sig":
                from sigbot.mm_risk import check_dispatch

                withdrawal = check_dispatch(self.planner, s, account, intent, order)
                if withdrawal:
                    # SEND_PENDING was never sent. Commit the risk latch and
                    # proven-unsent release together; SENDING/UNKNOWN return above.
                    order.state, order.request_state = "CANCELED", "RESOLVED"
                    order.reserve_cash, order.reserve_quantity, row.active = ZERO, ZERO, False
                    s.get(Outbox, order.id).state = "CANCELED_UNSENT"
                    s.add(Audit(account_id=account.id, at=self.clock.now,
                                kind="MM_RISK_UNSENT_WITHDRAWAL",
                                data={"order_id":order.id, **withdrawal}))
                    self.planner.on_unsent_canceled(s, account, intent, order)
                    return None
            self.planner.check_dispatch(s, account, intent, order)
            if not self.exchange.synthetic:
                from sigbot.live import SigExecutionExchange

                assert isinstance(self.exchange, SigExecutionExchange)
                self.exchange.prepare(s, account, intent, order)
            self.scheduler.charge_in_transaction(s, account, "NEW")
            request = OrderRequest(
                order.id,
                account.id,
                account.tournament,
                order.instrument_id,
                order.side,
                order.quantity,
                order.price,
                order.fee_bound,
                synthetic=intent.synthetic,
            )
            order.request_state = "SENDING"
            out = s.get(Outbox, order.id)
            assert out is not None
            out.state = "SENDING"
            s.add(
                RequestAttempt(
                    order_id=order.id, action="SUBMIT", state="SENDING", sent_at=self.clock.now
                )
            )
        # Crash here is SENDING on disk; recovery queries before doing anything else.
        if fault:
            fault("before_network")
        try:
            self.leader.check()
            observation = self.exchange.submit_limit(request)
            if fault:
                fault("after_network")
            self.leader.check()
            self.observe(order_id, observation)
            return observation
        except TransportUnknown:
            self.mark_unknown(order_id, "SUBMIT_UNKNOWN")
            return None
        except SubmissionNotSent as exc:
            self.resolve_not_sent(order_id, {"local_code": exc.code, "phase": "HTTP_PREPARATION"})
            return None
        except OrderRejected:
            with self.store.transaction() as s:
                account = self.store.lock_account(s, self.account_id)
                order = s.get(Order, order_id)
                assert order is not None
                order.state, order.request_state = "REJECTED", "RESOLVED"
                order.reserve_cash, order.reserve_quantity = ZERO, ZERO
                row, out = s.get(IntentRow, order.intent_id), s.get(Outbox, order.id)
                assert row is not None and out is not None
                row.active, out.state = False, "REJECTED"
                for attempt in s.scalars(
                    select(RequestAttempt).where(
                        RequestAttempt.order_id == order.id, RequestAttempt.state == "SENDING"
                    )
                ):
                    attempt.state = "REJECTED"
                self.planner.on_unsent_expired(s, account, intent, order)
                self.store.audit(
                    s,
                    account.id,
                    "OFFICIAL_PLACEMENT_REJECTED",
                    {"order": order_id},
                    self.clock.now,
                )
            return None
        except StorageUnavailable:
            self.mark_unknown(order_id, "EXECUTOR_LOCK_LOST")
            raise
        except (SchemaMismatch, Conflict):
            self.mark_unknown(order_id, "LIVE_RESPONSE_QUARANTINED")
            raise
        except (AuthorizationRequired, ContractNotVerified, RateLimited):
            self.mark_unknown(order_id, "SUBMIT_NOT_RESOLVED")
            raise

    def mark_unknown(self, order_id: str, reason: str) -> None:
        with self.store.transaction() as s:
            account = self.store.lock_account(s, self.account_id)
            order = s.get(Order, order_id)
            assert order is not None
            order.request_state = "UNKNOWN"
            from sigbot.uncertainty import order_scope

            scope = order_scope(s, account, order)
            if not self.config.parallel_strategy_execution or not scope["isolatable"]:
                account.lifecycle, account.reason = "PAUSED", reason
            out = s.get(Outbox, order_id)
            if out:
                out.state = "UNKNOWN"
            self.store.audit(
                s,
                account.id,
                "UNKNOWN",
                {"order": order_id, "reason": reason, "scope": scope},
                self.clock.now,
            )

    def resolve_not_sent(self, order_id: str, proof: dict) -> None:
        """Only caller-proven pre-HTTP failures; unknown network outcomes never use this."""
        with self.store.transaction() as s:
            account = self.store.lock_account(s, self.account_id)
            order = s.get(Order, order_id)
            if order is None or order.account_id != account.id or order.remote_id or order.filled:
                raise Conflict("NOT_SENT_ORDER_SCOPE_OR_REMOTE_STATE_CONFLICT")
            if order.request_state == "RESOLVED":
                return
            row = s.get(IntentRow, order.intent_id)
            assert row is not None
            intent = Intent.model_validate(row.payload)
            order.state, order.request_state = "CANCELED", "RESOLVED"
            order.reserve_cash, order.reserve_quantity, row.active = ZERO, ZERO, False
            outbox = s.get(Outbox, order.id)
            assert outbox is not None
            outbox.state = "NOT_SENT"
            for attempt in s.scalars(
                select(RequestAttempt).where(
                    RequestAttempt.order_id == order.id, RequestAttempt.state == "SENDING"
                )
            ):
                attempt.state = "NOT_SENT"
            self.planner.on_unsent_expired(s, account, intent, order)
            if account.lifecycle == "PAUSED" and account.reason in {
                "SUBMIT_UNKNOWN",
                "SUBMIT_NOT_RESOLVED",
                "ABSENCE_NOT_PROOF_OF_NON_ACCEPTANCE",
            }:
                account.reason = "PRE_HTTP_REQUEST_RECOVERED"
            self.store.audit(
                s, account.id, "HTTP_PROVEN_NOT_SENT", {"order": order.id, **proof}, self.clock.now
            )

    def observe(self, order_id: str, observation: Observation, fault=None) -> None:
        try:
            self._observe(order_id, observation, fault)
        except (SchemaMismatch, Conflict):
            self.controls.pause(self.account_id, "OBSERVATION_QUARANTINED")
            raise

    def _observe(self, order_id: str, observation: Observation, fault=None) -> None:
        with self.store.transaction() as s:
            account = self.store.lock_account(s, self.account_id)
            order = s.get(Order, order_id)
            if order is None or order.account_id != self.account_id:
                raise Conflict("ORDER_SCOPE_MISMATCH")
            if (
                observation.logical_id,
                observation.instrument_id,
                observation.side,
                observation.quantity,
                observation.limit_price,
            ) != (order.id, order.instrument_id, order.side, order.quantity, order.price):
                raise SchemaMismatch("UNEXPECTED_ORDER_NORMALIZATION")
            if (
                observation.order_id.startswith("SIM-") != self.exchange.synthetic
                or observation.revision < 0
                or (not self.exchange.synthetic and not observation.order_id.isdecimal())
            ):
                raise SchemaMismatch("SYNTHETIC_SCOPE_MISMATCH")
            if order.remote_id and order.remote_id != observation.order_id:
                raise SchemaMismatch("REMOTE_ORDER_CONFLICT")
            payload = json.loads(json.dumps(asdict(observation), default=str))
            # Order revision and fill ingestion are independent evidence streams.
            # History may supply missing fills without changing the order revision.
            payload.pop("fills")
            event_id = observation.order_id + ":" + str(observation.revision)
            old = s.get(Inbox, (account.id, event_id))
            if old:
                if old.digest != content_hash(payload):
                    raise Conflict("ORDER_EVENT_CONTENT_CONFLICT")
            new_state = merge_state(order.state, order.revision, observation)
            order.remote_id = observation.order_id
            for fill in observation.fills:
                self.ledger.fill(s, account, order, fill, fault)
            if observation.revision > order.revision:
                if observation.filled < order.filled or observation.filled < order.observed_filled:
                    raise SchemaMismatch("CUMULATIVE_FILL_DECREASE")
                order.observed_filled = observation.filled
                order.state, order.revision = new_state, observation.revision
            order.request_state = "ACKNOWLEDGED"
            out = s.get(Outbox, order_id)
            assert out is not None
            out.state = "ACKNOWLEDGED"
            pending = order.quantity - order.filled
            # Only complete fill evidence plus a terminal observation releases unused reservation.
            if order.observed_filled > order.filled:
                account.lifecycle, account.reason = "PAUSED", "MISSING_FILL_EVIDENCE"
                pending = max(pending, order.observed_filled - order.filled)
            elif order.state in TERMINAL:
                pending = ZERO
                order.request_state = "RESOLVED"
                row = s.get(IntentRow, order.intent_id)
                assert row is not None
                row.active = False
                if order.cancel_state is not None:
                    order.cancel_state = "CANCEL_CONFIRMED"
            order.reserve_cash = (
                pending * (order.price + order.fee_bound / order.quantity)
                if order.side == "BUY"
                else ZERO
            )
            order.reserve_quantity = pending if order.side == "SELL" else ZERO
            self.planner.on_observed(s, account, order, observation)
            origin = s.get(IntentRow, order.intent_id)
            if origin.payload.get("strategy_id") == "mm_sig" and self.config.mm_risk_policy is not None:
                from sigbot.mm_risk import observed

                observed(s, account, order, self.config.mm_risk_policy, self.clock.now)
            if not old:
                s.add(
                    Inbox(
                        account_id=account.id,
                        event_id=event_id,
                        digest=content_hash(payload),
                        payload=payload,
                    )
                )
            for attempt in s.scalars(
                select(RequestAttempt).where(
                    RequestAttempt.order_id == order_id, RequestAttempt.state == "SENDING"
                )
            ):
                attempt.state = (
                    "RESULT_UNCONFIRMED"
                    if attempt.action == "CANCEL" and order.state not in TERMINAL
                    else "OBSERVED"
                )
            self.store.audit(
                s,
                account.id,
                "ORDER_OBSERVED",
                {
                    "order": order_id,
                    "state": order.state,
                    "filled": str(order.filled),
                    "revision": order.revision,
                },
                self.clock.now,
            )

    def resolve(self, order_id: str) -> bool:
        with self.account_operation(order_id=order_id):
            return self._resolve(order_id)

    def _resolve(self, order_id: str) -> bool:
        with self.store.transaction() as s:
            order = s.get(Order, order_id)
            if order is None or order.account_id != self.account_id:
                raise Conflict("ORDER_SCOPE_MISMATCH")
            if (
                order.state in TERMINAL
                and order.request_state == "RESOLVED"
                and not (order.reserve_cash or order.reserve_quantity)
            ):
                return True
        self.scheduler.charge(self.account_id, "READ")
        observation = self.exchange.get_order(order_id)
        if observation is None:
            from sigbot.live import SigExecutionExchange
            from sigbot.recovery import prove_expired_no_order

            if isinstance(self.exchange, SigExecutionExchange):
                proof = prove_expired_no_order(self.exchange, order_id)
                if proof is not None:
                    self.resolve_expired_no_order(order_id, proof)
                    return True
            self.mark_unknown(order_id, "ABSENCE_NOT_PROOF_OF_NON_ACCEPTANCE")
            return False
        self.observe(order_id, observation)
        return True

    def resolve_expired_no_order(self, order_id, proof):
        """Close only the local economic risk; do not invent an official receipt/fill."""
        from sigbot.storage import LiveWire, Position

        with self.store.transaction() as s:
            account = self.store.lock_account(s, self.account_id)
            order, wire = s.get(Order, order_id), s.get(LiveWire, order_id)
            if order is None or wire is None or order.account_id != account.id:
                raise Conflict("EXPIRED_PROOF_ORDER_SCOPE_MISMATCH")
            if order.request_state == "RESOLVED":
                return
            self.clock.sync()
            local_positions = {}
            for pos in s.scalars(select(Position).where(Position.account_id == account.id)):
                if pos.quantity:
                    inst = s.get(Instrument, pos.instrument_id)
                    if inst is None or inst.exchange_id is None:
                        raise Conflict("EXPIRED_PROOF_POSITION_MAPPING_REQUIRED")
                    if inst.exchange_id + ":" + inst.outcome in local_positions:
                        raise Conflict("EXPIRED_PROOF_POSITION_ALIAS_CONFLICT")
                    local_positions[inst.exchange_id + ":" + inst.outcome] = {
                        "quantity": str(pos.quantity),
                        "cost": str(pos.cost),
                    }
            expected = {
                key: (Decimal(value["quantity"]), Decimal(value["cost"]))
                for key, value in proof["positions"].items()
                if Decimal(value["quantity"])
            }
            actual = {
                key: (Decimal(value["quantity"]), Decimal(value["cost"]))
                for key, value in local_positions.items()
            }
            if (
                proof.get("kind") != "EXPIRED_SUBMIT_NO_VENUE_EFFECT"
                or order.request_state not in {"UNKNOWN", "SENDING"}
                or proof["account_id"] != account.id
                or proof["tournament_id"] != account.tournament
                or proof["order_id"] != order_id
                or wire.digest != proof["wire_digest"]
                or content_hash(wire.payload) != wire.digest
                or wire.receipt
                or order.remote_id
                or order.filled
                or order.cancel_state
                or not 0
                <= (self.clock.now - utc_time(proof["observed_at"])).total_seconds()
                <= self.config.account_ttl_seconds
                or actual != expected
                or account.cash != Decimal(proof["projected_cash"])
                or proof["layer"]
                != ("MOCK" if getattr(self.exchange, "mock", False) else "REAL_READ")
            ):
                raise Conflict("EXPIRED_NO_ORDER_PROOF_CHANGED_OR_STALE")
            order.state, order.request_state = "CLOSED", "RESOLVED"
            order.reserve_cash, order.reserve_quantity = ZERO, ZERO
            row, outbox = s.get(IntentRow, order.intent_id), s.get(Outbox, order.id)
            assert row is not None and outbox is not None
            row.active, outbox.state = False, "NO_VENUE_EFFECT_CONFIRMED"
            for attempt in s.scalars(
                select(RequestAttempt).where(
                    RequestAttempt.order_id == order_id, RequestAttempt.state == "SENDING"
                )
            ):
                attempt.state = "EXPIRED_NO_VENUE_EFFECT_CONFIRMED"
            self.planner.on_observed(s, account, order, None)
            account.lifecycle, account.reason, account.reconciled_at = (
                "PAUSED",
                "EXPIRED_UNKNOWN_NO_ORDER_CONFIRMED",
                None,
            )
            self.store.audit(
                s,
                account.id,
                "EXPIRED_UNKNOWN_NO_ORDER_CONFIRMED",
                {"order": order_id, **proof},
                self.clock.now,
            )

    def retry_pending_cancel(self, order_id: str) -> bool:
        """Retry only an identified, still-open order's existing cancel intent."""
        with self.store.transaction() as session:
            order = session.get(Order, order_id)
            if (
                order is None
                or order.account_id != self.account_id
                or not order.remote_id
                or order.state not in {"OPEN", "PARTIAL"}
                or order.request_state != "ACKNOWLEDGED"
                or order.cancel_state not in {"CANCEL_UNKNOWN", "CANCEL_REQUESTED"}
            ):
                return False
            last = session.scalar(
                select(RequestAttempt.sent_at)
                .where(RequestAttempt.order_id == order_id, RequestAttempt.action == "CANCEL")
                .order_by(RequestAttempt.sent_at.desc())
                .limit(1)
            )
            if last is None or (self.clock.now - last).total_seconds() < 30:
                return False
        # The caller just queried this order; cancel retains OMS authority,
        # budget and claims. No new submit, guessed ID or early reserve release.
        return self.cancel(order_id)

    def cancel(self, order_id: str, before_send=None) -> bool:
        with self.account_operation(order_id=order_id):
            return self._cancel(order_id, before_send)

    def _cancel(self, order_id: str, before_send=None) -> bool:
        self.execution_permission()
        self.ownership()
        with self.store.transaction() as s:
            account = self.store.lock_account(s, self.account_id)
            order = s.get(Order, order_id)
            if order is None or order.account_id != account.id:
                raise Conflict("BOT_TRACKED_CANCEL_SCOPE_REQUIRED")
            if order.state in TERMINAL and not (order.reserve_cash or order.reserve_quantity):
                return True
            if before_send is not None and not before_send(s, order):
                return False
            if order.request_state == "SEND_PENDING":
                row = s.get(IntentRow, order.intent_id)
                assert row is not None
                row.active = False
                order.state, order.request_state = "CANCELED", "RESOLVED"
                order.reserve_cash, order.reserve_quantity = ZERO, ZERO
                out = s.get(Outbox, order.id)
                assert out is not None
                out.state = "CANCELED_UNSENT"
                self.planner.on_unsent_canceled(
                    s, account, Intent.model_validate(row.payload), order
                )
                return True
            if order.remote_id is None:
                return False
            if self.clock.now >= self.planner.ends_at:
                raise RiskRejected("TRADING_CLOSED")
            if not self.exchange.synthetic:
                from sigbot.live import SigExecutionExchange

                assert isinstance(self.exchange, SigExecutionExchange)
                self.exchange.before_cancel(s, account, order)
            self.scheduler.charge_in_transaction(s, account, "CANCEL")
            order.cancel_state = "CANCEL_REQUESTED"
            remote_id = order.remote_id
            s.add(
                RequestAttempt(
                    order_id=order.id, action="CANCEL", state="SENDING", sent_at=self.clock.now
                )
            )
        try:
            self.leader.check()
            observation = self.exchange.cancel_order(remote_id)
            self.leader.check()
            self.observe(order_id, observation)
            return observation.state in TERMINAL
        except SubmissionNotSent as exc:
            with self.store.transaction() as s:
                account = self.store.lock_account(s, self.account_id)
                order = s.get(Order, order_id)
                assert order is not None
                order.cancel_state = None
                for attempt in s.scalars(
                    select(RequestAttempt).where(
                        RequestAttempt.order_id == order.id,
                        RequestAttempt.action == "CANCEL",
                        RequestAttempt.state == "SENDING",
                    )
                ):
                    attempt.state = "NOT_SENT"
                self.store.audit(
                    s,
                    account.id,
                    "CANCEL_HTTP_NOT_SENT",
                    {"order": order.id, "code": exc.code},
                    self.clock.now,
                )
            return False
        except (
            TransportUnknown,
            StorageUnavailable,
            SchemaMismatch,
            Conflict,
            ContractNotVerified,
            AuthorizationRequired,
            RateLimited,
        ):
            with self.store.transaction() as s:
                a = self.store.lock_account(s, self.account_id)
                order = s.get(Order, order_id)
                assert order is not None
                order.cancel_state = "CANCEL_UNKNOWN"
                from sigbot.uncertainty import order_scope

                if (
                    not self.config.parallel_strategy_execution
                    or not order_scope(s, a, order)["isolatable"]
                ):
                    a.lifecycle, a.reason = "PAUSED", "CANCEL_UNKNOWN"
            return False


def cumulative_deltas(values: list[Decimal]) -> list[Decimal]:
    last, result = ZERO, []
    for value in values:
        if not value.is_finite() or value < last:
            raise SchemaMismatch("CUMULATIVE_FILL_DECREASE")
        result.append(value - last)
        last = value
    return result
