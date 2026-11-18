"""Continuous ECR scheduling through the shared allocator, Executor and ledger.

This module owns no alternative trading client. Recovery only queries requests;
startup resume is explicit and a manual pause is never silently cleared.
"""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

from sqlalchemy import select

from sigbot.domain import TERMINAL, utc_time
from sigbot.errors import (
    AuthorizationRequired,
    Conflict,
    RiskRejected,
    SigbotError,
    StorageUnavailable,
)
from sigbot.storage import Account, Audit, Instrument, IntentRow, Order, StrategyEvent
from sigbot.strategies.ecr_sig_v1 import core
from sigbot.strategies.ecr_sig_v1.inputs import AccountInput, Snapshot, epoch
from sigbot.strategies.ecr_sig_v1.oms import EcrPlanner, entry_intent


class EcrRuntime:
    def __init__(
        self, executor, reference: AccountInput, *, refresh_book=None, order_ttl=120, run_until=None
    ):
        if order_ttl <= 0:
            raise ValueError("positive resting-order lifetime required")
        self.executor, self.store, self.clock = executor, executor.store, executor.clock
        self.config, self.exchange = executor.config, executor.exchange
        self.account_id = executor.account_id
        self.policy = EcrPlanner(
            self.store,
            self.config,
            self.clock,
            executor.planner.starts_at,
            executor.planner.ends_at,
        )
        self.reference = reference.model_copy(deep=True)
        self.refresh_book = refresh_book
        self.order_ttl = order_ttl  # Local resting-order policy, not SIG's plan TTL.
        self.next_reconcile = self.clock.now
        self.transient_pause = False
        self.last_result: dict = {}
        self.started = False
        self.run_until = utc_time(run_until) if run_until else None
        self.poll_cursor = 0

    def start(self, *, resume=False, operator="") -> dict:
        if not self.started:
            self.executor.start()
            self.started = True
        self.poll_orders()
        result = self.reconcile()
        if resume and result["status"] == "PASS":
            self.executor.execution_permission()
            self.executor.controls.resume(self.account_id, operator)
        self.last_result = {
            "startup": result,
            "account": self.executor.controls.status(self.account_id),
        }
        return self.last_result

    def reconcile(self):
        if self.config.parallel_strategy_execution:
            with self.store.transaction() as s:
                account = s.get(Account, self.account_id)
                if (
                    account.lifecycle == "HEALTHY"
                    and account.reconciled_at is not None
                    and 0 <= (self.clock.now - utc_time(account.reconciled_at)).total_seconds() < 25
                ):
                    previous = s.scalar(
                        select(Audit)
                        .where(Audit.account_id == self.account_id, Audit.kind == "RECONCILIATION")
                        .order_by(Audit.id.desc())
                        .limit(1)
                    )
                    if previous and previous.data.get("status") == "PASS":
                        self.next_reconcile = self.clock.now + timedelta(seconds=30)
                        return {
                            **previous.data,
                            "basis": "SHARED_EXISTING_REAL_ACCOUNT_RECONCILIATION",
                            "checked_at": utc_time(account.reconciled_at).isoformat(),
                        }
        result = self.executor.reconcile()
        self.next_reconcile = self.clock.now + timedelta(seconds=30)
        if result["status"] != "PASS":
            self.transient_pause = False
        return result

    def orders(self, *, ecr_only=False):
        with self.store.transaction() as s:
            items = []
            for order in s.scalars(select(Order).where(Order.account_id == self.account_id)):
                intent = s.get(IntentRow, order.intent_id)
                assert intent is not None
                if ecr_only and (
                    intent.payload.get("strategy_id"),
                    intent.payload.get("strategy_version"),
                ) != ("ecr_sig", "1.0"):
                    continue
                if (
                    order.state not in TERMINAL
                    or order.request_state != "RESOLVED"
                    or order.cancel_state in {"CANCEL_REQUESTED", "CANCEL_UNKNOWN"}
                    or order.reserve_cash
                    or order.reserve_quantity
                ):
                    items.append((order, dict(intent.payload)))
            return items

    def poll_orders(self):
        items = self.orders()
        if not self.exchange.synthetic and items:
            # Current coherent checkpoints also ingest active fills. One fair
            # explicit query per tick leaves room in the shared account budget.
            items = [items[self.poll_cursor % len(items)]]
            self.poll_cursor += 1
        for order, _ in items:
            if order.request_state != "SEND_PENDING":
                self.executor.resolve(order.id)

    def account_reference(self):
        """Persist initial capital, peak equity and daily opening equity across restarts."""
        with self.store.transaction() as s:
            account = self.store.lock_account(s, self.account_id)
            previous = s.scalar(
                select(Audit)
                .where(Audit.account_id == account.id, Audit.kind == "ECR_RUNTIME_RISK_REFERENCE")
                .order_by(Audit.id.desc())
                .limit(1)
            )
            data = previous.data if previous else {}
            initial = Decimal(data.get("initial", str(self.reference.initial)))
            if initial != self.reference.initial:
                raise RiskRejected("ECR_RUNTIME_INITIAL_CAPITAL_CHANGED")
            result = self.reference.model_copy(deep=True)
            result.initial, result.cash = initial, account.cash
            result.high_water = max(result.high_water, Decimal(data.get("high_water", "0")))
            result.day_start_equity = Decimal(
                data.get("day_start_equity", str(result.day_start_equity))
            )
            marked = self.policy.shared_account(s, account, result)
            day = self.clock.now.date().isoformat()
            if data and data["utc_day"] != day:
                result.day_start_equity = marked.equity
            result.high_water = max(result.high_water, marked.equity)
            result.equity = marked.equity
            result.reconciled = account.lifecycle == "HEALTHY"
            result.unknown_writes = marked.unknown_writes
            self.store.audit(
                s,
                account.id,
                "ECR_RUNTIME_RISK_REFERENCE",
                {
                    "initial": str(initial),
                    "high_water": str(result.high_water),
                    "day_start_equity": str(result.day_start_equity),
                    "utc_day": day,
                },
                self.clock.now,
            )
            return result

    def submit(self, intent):
        if self.run_until:
            if self.clock.now >= self.run_until:
                raise RiskRejected("AUTOMATIC_RUN_DEADLINE_REACHED")
            intent = intent.model_copy(
                update={"expires_at": min(intent.expires_at, self.run_until)}
            )
        oid = self.executor.planner.reserve(intent)
        if oid:
            self.executor.dispatch(oid)
            status = self.executor.controls.status(self.account_id)
            if status["unknown_count"] or status["cancel_pending_count"]:
                self.transient_pause = True
        return oid

    def refresh(self, instrument_id):
        if self.refresh_book is not None:
            self.refresh_book(instrument_id)

    def exits(self, references, result):
        with self.store.transaction() as s:
            events = list(
                s.scalars(
                    select(StrategyEvent).where(
                        StrategyEvent.account_id == self.account_id,
                        StrategyEvent.strategy_id == "ecr_sig",
                        StrategyEvent.strategy_version == "1.0",
                        StrategyEvent.state != "CLOSED",
                    )
                )
            )
        for event in events:
            reference = references.get(event.event_key)
            with self.store.transaction() as s:
                current = self.policy.event(s, self.account_id, event.event_key)
                assert current is not None
                data = dict(current.data)
                if reference is None:
                    data.setdefault("reference_missing_since", self.clock.now.isoformat())
                else:
                    data.pop("reference_missing_since", None)
                current.data = data
                missing = (
                    int(
                        (self.clock.now - utc_time(data["reference_missing_since"])).total_seconds()
                    )
                    if "reference_missing_since" in data
                    else 0
                )
            active = [
                (o, p)
                for o, p in self.orders(ecr_only=True)
                if o.instrument_id == event.instrument_id
            ]
            if any(o.side == "SELL" for o, _ in active):
                continue
            self.refresh(event.instrument_id)
            reason, intent = self.policy.exit_intent(event.event_key, reference, missing)
            result["exits"][event.event_key] = reason
            if reason == "EXIT_BLOCKED" or intent is None:
                continue
            # A partially filled entry may still be accumulating risk. Confirm its
            # remainder canceled and ingest racing fills before sizing the SELL.
            for order, _ in active:
                if not self.executor.cancel(order.id):
                    self.transient_pause = True
                    return
            reason, intent = self.policy.exit_intent(event.event_key, reference, missing)
            if intent is not None:
                result["exit_orders"].append(self.submit(intent))

    def entries(self, snapshot, result):
        if (snapshot.synthetic, snapshot.account_id, snapshot.tournament_id) != (
            self.exchange.synthetic,
            self.account_id,
            self.config.expected_tournament_id,
        ):
            raise AuthorizationRequired("ECR_RUNTIME_INPUT_SCOPE_MISMATCH")
        if not snapshot.candidates:
            return  # Recovery/exits still run; no entry means no entry-risk calculation.
        if snapshot.now > self.clock.now or (self.clock.now - snapshot.now).total_seconds() > 5:
            raise RiskRejected("ECR_RUNTIME_SNAPSHOT_STALE_OR_FUTURE")
        reference = self.account_reference()
        with self.store.transaction() as s:
            account = self.store.lock_account(s, self.account_id)
            shared = self.policy.shared_account(s, account, reference)
            candidates = []
            for raw in snapshot.candidates:
                try:
                    candidates.append(raw.candidate(self.clock.now, snapshot.synthetic))
                except core.Reject as exc:
                    result["rejected"][raw.key] = str(exc)
            planned, rejected = core.allocate(candidates, shared, epoch(self.clock.now))
            result["rejected"].update(rejected)
        result["qualified_candidates"] = len(planned)
        indexed = {c.key: c for c in snapshot.candidates}
        for proposed in planned:
            raw = indexed[proposed.key]
            try:
                self.refresh(raw.instrument_id)
                with self.store.transaction() as s:
                    account = self.store.lock_account(s, self.account_id)
                    inst = s.get(Instrument, raw.instrument_id)
                    if inst is None:
                        raise RiskRejected("ECR_RUNTIME_INSTRUMENT_NOT_STAGED")
                    candidate = raw.candidate(self.clock.now, snapshot.synthetic)
                    from dataclasses import replace

                    candidate = replace(
                        candidate,
                        sig_book=core.Book(
                            tuple(
                                core.Level(Decimal(x["price"]), Decimal(x["quantity"]))
                                for x in inst.book.get("bids", [])
                            ),
                            tuple(
                                core.Level(Decimal(x["price"]), Decimal(x["quantity"]))
                                for x in inst.book.get("asks", [])
                            ),
                            epoch(utc_time(inst.book_at)) if inst.book_at else 0,
                            inst.quality == "HEALTHY",
                        ),
                    )
                    event = self.policy.event(s, account.id, candidate.event_key)
                    if event and event.last_exit_at:
                        candidate = replace(
                            candidate, last_event_exit_at=epoch(utc_time(event.last_exit_at))
                        )
                    proposal = core.plan(
                        candidate,
                        self.policy.shared_account(s, account, reference),
                        epoch(self.clock.now),
                    )
                current = snapshot.model_copy(update={"now": self.clock.now, "account": reference})
                result["entry_orders"].append(self.submit(entry_intent(current, raw, proposal)))
                if self.executor.controls.status(self.account_id)["lifecycle"] != "HEALTHY":
                    break
            except (core.Reject, RiskRejected, Conflict) as exc:
                result["rejected"][raw.key] = str(exc)

    def idle(self, snapshot=None):
        if (
            not self.config.parallel_strategy_execution
            or self.transient_pause
            or snapshot is not None
            and snapshot.candidates
            or self.orders(ecr_only=True)
        ):
            return False
        with self.store.transaction() as session:
            exposure = session.scalar(
                select(StrategyEvent)
                .where(
                    StrategyEvent.account_id == self.account_id,
                    StrategyEvent.strategy_id == "ecr_sig",
                    StrategyEvent.state != "CLOSED",
                )
                .limit(1)
            )
        return exposure is None

    def tick(self, snapshot: Snapshot | None = None, references=None) -> dict:
        result = {
            "at": self.clock.now.isoformat(),
            "qualified_candidates": 0,
            "entry_orders": [],
            "exit_orders": [],
            "exits": {},
            "rejected": {},
        }
        try:
            self.executor.ownership()
            if self.idle(snapshot):
                result["account_observation"] = "SHARED_LEDGER_ONLY_NO_NEW_RECONCILIATION"
                return self.record_result(result)
            self.poll_orders()  # Query first; never dispatch SENDING/UNKNOWN again.
            for order, payload in self.orders(ecr_only=True):
                if (
                    order.request_state == "SEND_PENDING"
                    and utc_time(payload["expires_at"]) <= self.clock.now
                ):
                    self.executor.dispatch(order.id)  # Existing durable unsent-expiry path.
            status = self.executor.controls.status(self.account_id)
            transient_read = status["reason"].startswith("ECR_RUNTIME:") and status["reason"].split(
                ":", 1
            )[1] in {
                "RATE_LIMITED",
                "SHARED_READ_BUDGET_EXHAUSTED",
                "READ_SESSION_LIMIT_REACHED",
                "TRANSPORT_UNKNOWN",
            }
            if self.clock.now >= self.next_reconcile or self.transient_pause or transient_read:
                reconciliation = self.reconcile()
                result["reconciliation"] = reconciliation
                if (
                    (self.transient_pause or transient_read)
                    and reconciliation["status"] == "PASS"
                    and (status["reason"] in {"SUBMIT_UNKNOWN", "CANCEL_UNKNOWN"} or transient_read)
                    and not status["unknown_count"]
                    and not status["cancel_pending_count"]
                ):
                    self.executor.controls.resume(
                        self.account_id, "ECR_CONFIRMED_TRANSIENT_RECOVERY"
                    )
                    self.transient_pause = False
            if self.executor.controls.status(self.account_id)["lifecycle"] == "HEALTHY" and (
                self.run_until is None or self.clock.now < self.run_until
            ):
                for order, payload in self.orders(ecr_only=True):
                    if (
                        order.request_state != "SEND_PENDING"
                        and (self.clock.now - utc_time(payload["created_at"])).total_seconds()
                        >= self.order_ttl
                    ):
                        if not self.executor.cancel(order.id):
                            self.transient_pause = True
                            break
                if self.executor.controls.status(self.account_id)["lifecycle"] == "HEALTHY":
                    self.exits(references() if callable(references) else references or {}, result)
                if (
                    snapshot is not None
                    and self.executor.controls.status(self.account_id)["lifecycle"] == "HEALTHY"
                ):
                    self.entries(snapshot, result)
        except SigbotError as exc:
            result["blocked"] = exc.code
            if exc.code not in {
                "ACCOUNT_OPERATION_BUSY",
                "LIVE_ORDER_READ_WINDOW_CHANGED",
                "READ_SESSION_LIMIT_REACHED",
                "SHARED_READ_BUDGET_EXHAUSTED",
            } and not isinstance(exc, (RiskRejected, Conflict)):
                self.executor.controls.pause(self.account_id, "ECR_RUNTIME:" + exc.code)
        return self.record_result(result)

    def record_result(self, result):
        result["account"] = self.executor.controls.status(self.account_id)
        result["status"] = "RUNNING" if result["account"]["lifecycle"] == "HEALTHY" else "PAUSED"
        result["layer"] = (
            "MOCK"
            if self.exchange.synthetic or getattr(self.exchange, "mock", False)
            else self.config.mode.value
        )
        result["real_submit_attempts"] = (
            0 if result["layer"] == "MOCK" else self.exchange.call_count["POST"]
        )
        result["real_cancel_attempts"] = (
            0 if result["layer"] == "MOCK" else self.exchange.call_count["DELETE"]
        )
        with self.store.transaction() as s:
            self.store.audit(s, self.account_id, "ECR_RUNTIME_CYCLE", result, self.clock.now)
        self.last_result = result
        return result

    def close(self):
        # A stop records outstanding risk; it never fabricates cancellation.
        if self.started:
            try:
                self.executor.ownership()
                if self.config.parallel_strategy_execution:
                    with self.store.transaction() as s:
                        self.store.audit(
                            s,
                            self.account_id,
                            "STRATEGY_WORKER_STOPPED",
                            {"strategy_id": "ecr_sig"},
                            self.clock.now,
                        )
                else:
                    self.executor.controls.pause(self.account_id, "ECR_RUNTIME_STOPPED")
            except StorageUnavailable:
                pass  # Lost ownership never edits another executor's account state.
        self.executor.close()
        self.started = False
