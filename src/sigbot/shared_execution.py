"""The single credential-bearing consumer for all registered account strategies."""

from __future__ import annotations

import argparse
import json
import signal
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from sqlalchemy import select

from sigbot.account_consumer import AccountConsumer
from sigbot.authorization import Approval
from sigbot.config import Config
from sigbot.contracts import Contract
from sigbot.domain import TERMINAL, Clock, utc_time
from sigbot.domain import content_hash as document_hash
from sigbot.errors import (
    AuthorizationRequired,
    ContractNotVerified,
    RateLimited,
    RiskRejected,
    SchemaMismatch,
    SigbotError,
    TransportUnknown,
)
from sigbot.execution import Executor
from sigbot.live import SigExecutionExchange, bindings
from sigbot.official import OfficialSchema
from sigbot.readcheck import load_key
from sigbot.shared_marketdata import account_read_priority
from sigbot.storage import (
    ExecutionCommand,
    Instrument,
    IntentRow,
    Order,
    Store,
    StrategyEvent,
)
from sigbot.strategies.ecr_sig_v1.observer import atomic_json
from sigbot.strategies.mm_sig_v1.live.execution import RealExchange
from sigbot.strategies.mm_sig_v1.live.fees import load_fee
from sigbot.strategies.router import StrategyRouter


class Authority:
    def __init__(self, item):
        self.identity = item["strategy_id"], item["strategy_version"]
        self.config = Config.load(item["application_config"])
        raw = json.loads(Path(item["approval_file"]).read_text())
        self.eligibility_confirmed = raw.pop("eligibility_confirmed_by_operator", False)
        self.approval = Approval.model_validate(raw)
        self.contract = Contract(self.config.contract_file)
        if self.contract.data.get("schema_path"):
            self.schema = OfficialSchema(self.contract.data["schema_path"])
        elif self.identity[0] == "mm_sig":
            from sigbot.strategies.mm_sig_v1.data import reviewed_schema

            _, self.schema = reviewed_schema()
        else:
            raise AuthorizationRequired("SHARED_REVIEWED_SCHEMA_REQUIRED")
        self.fee = load_fee(self.config.live_fee_file)

    def check_binding(self):
        if any(getattr(self.approval, k) != v for k, v in bindings(self.config).items()):
            raise AuthorizationRequired("SHARED_AUTHORITY_BINDING_CHANGED")


class Authorities:
    """One writer/key; original strategy configurations and budgets stay separate."""

    def __init__(self, items):
        loaded = [Authority(i) for i in items]
        self.items = {a.identity: a for a in loaded}
        if len(self.items) != len(loaded) or not loaded:
            raise AuthorizationRequired("SHARED_AUTHORITY_IDENTITIES_REQUIRED")
        self.primary = loaded[0]
        first = self.primary.config
        for a in loaded:
            cfg = a.config
            if (cfg.expected_account_id, cfg.expected_tournament_id, cfg.database_url) != (
                first.expected_account_id,
                first.expected_tournament_id,
                first.database_url,
            ):
                raise AuthorizationRequired("SHARED_AUTHORITY_ACCOUNT_OR_DATABASE_MISMATCH")
            if (cfg.read_budget, cfg.write_budget, cfg.new_order_budget) != (
                first.read_budget,
                first.write_budget,
                first.new_order_budget,
            ):
                raise AuthorizationRequired("SHARED_ACCOUNT_RATE_BUDGET_MISMATCH")
            if cfg.strategy_capital_allocations != first.strategy_capital_allocations:
                raise AuthorizationRequired("SHARED_STRATEGY_CAPITAL_ALLOCATION_MISMATCH")
        mm_policies = {
            a.config.mm_risk_policy.digest if a.config.mm_risk_policy else None
            for a in loaded
            if a.identity[0] == "mm_sig"
        }
        if len(mm_policies) > 1:
            raise AuthorizationRequired("SHARED_MM_RISK_POLICY_MISMATCH")
        if not self.primary.eligibility_confirmed:
            raise AuthorizationRequired("SHARED_ACCOUNT_ELIGIBILITY_CONFIRMATION_REQUIRED")

    @contextmanager
    def activate(self, executor, identity):
        authority = self.items.get(identity)
        if authority is None:
            raise AuthorizationRequired("SHARED_STRATEGY_AUTHORITY_NOT_REGISTERED")
        exchange = executor.exchange
        previous = (
            executor.config,
            exchange.config,
            exchange.approval,
            exchange.contract,
            exchange.schema,
            exchange.fee,
            exchange.empirical_fee_accepted_by_operator,
        )
        cfg = authority.config
        executor.config = executor.planner.config = executor.scheduler.config = cfg
        exchange.config, exchange.approval = cfg, authority.approval
        exchange.contract, exchange.schema, exchange.fee = (
            authority.contract,
            authority.schema,
            authority.fee,
        )
        exchange.empirical_fee_accepted_by_operator = authority.approval.accept_empirical_fee_model
        exchange.reader.config = exchange.reader.transport.config = cfg
        exchange.reader.contract = exchange.reader.transport.contract = authority.contract
        exchange.reader.schema = authority.schema
        try:
            yield authority
        finally:
            (
                executor.config,
                exchange.config,
                exchange.approval,
                exchange.contract,
                exchange.schema,
                exchange.fee,
                exchange.empirical_fee_accepted_by_operator,
            ) = previous
            executor.planner.config = executor.scheduler.config = executor.config
            exchange.reader.config = exchange.reader.transport.config = exchange.config
            exchange.reader.contract = exchange.reader.transport.contract = exchange.contract
            exchange.reader.schema = exchange.schema


class SharedRuntime:
    def __init__(self, path, env_file, resume):
        self.document = json.loads(Path(path).read_text())
        self.authorities = Authorities(self.document["strategies"])
        self.clock, self.stop = Clock(realtime=True), threading.Event()
        primary = self.authorities.primary
        primary.check_binding()
        cfg = primary.config
        assert cfg.expected_account_id is not None
        self.store = Store(cfg.database_url, "live")
        self.exchange = RealExchange(
            self.store, cfg, self.clock, load_key(env_file), primary.approval
        )
        self.exchange.empirical_fee_accepted_by_operator = (
            primary.approval.accept_empirical_fee_model
        )
        self.exchange.authorized()
        cup = self.exchange.call(
            "tournament_identity", path_params={"slug": cfg.expected_tournament_slug}
        )
        router = StrategyRouter(
            self.store, cfg, self.clock, utc_time(cup["startDate"]), utc_time(cup["endDate"])
        )
        self.executor = Executor(
            self.store, cfg, self.clock, self.exchange, router, cfg.expected_account_id
        )
        self.consumer = AccountConsumer(
            self.executor, self.activate, self.refresh, self.refresh_ecr_reference
        )
        self.resume_requested = resume
        self.next_reconcile = self.clock.now
        self.last_reconciliation = {}
        self.poll_cursor = 0
        self.ecr_exits = None
        self.ssr_next = self.clock.now
        self.ssr_status: dict = {"status": "NOT_REGISTERED"}
        self.contract_review_error: str | None = None
        self.output = Path(self.document.get("output", "runtime/live/shared-executor-status.json"))
        self.grid = None
        if "grid" in self.document:
            from sigbot.strategies.grid_runtime import GridRunner

            self.grid = GridRunner(self, self.document["grid"], env_file)
        self.grid_v2 = None
        if "grid_v2" in self.document:
            from sigbot.strategies.grid_v2_runtime import RiskGridRunner

            if self.grid is None:
                raise RiskRejected("GRID_V2_SHARED_SAMPLER_REQUIRED")
            self.grid_v2 = RiskGridRunner(self, self.document["grid_v2"], env_file)

    def refresh_ecr_reference(self, instrument_id):
        from sigbot.strategies.ecr_sig_v1.reference_runtime import refresh_execution_reference

        refresh_execution_reference(self.store, self.executor.config, instrument_id)
        self.clock.sync()

    @contextmanager
    def activate(self, identity, action="SUBMIT"):
        with self.authorities.activate(self.executor, identity) as authority:
            authority.check_binding()
            self.exchange.authorized()
            deadline = (
                authority.approval.cleanup_until
                if action == "CANCEL"
                else authority.approval.new_orders_until
            )
            if action != "READ" and self.clock.now >= deadline:
                # Query recovery is independent; this scope is used for commands only.
                raise AuthorizationRequired("SHARED_STRATEGY_APPROVAL_EXPIRED")
            yield

    def check_current_reference(self):
        """Record document changes without changing verified execution admission."""
        from sigbot.strategies.mm_sig_v1.data import current_reference

        self.next_reference_check = self.clock.now + timedelta(seconds=60)
        try:
            self.reference_observation = current_reference(
                self.output.parent / "official-reference", self.authorities.primary.schema
            )
        except ContractNotVerified as exc:
            # Compatibility with an older observer; never reinterpret a document
            # difference as an execution or account failure.
            self.reference_observation = {
                "policy": "REPORT_ONLY",
                "status": "DOCUMENT_CHANGE_REPORTED",
                "code": exc.code,
            }
        self.contract_review_error = None
        self.exchange.contract_review_error = None

    def document_worker(self):
        while not self.stop.is_set():
            try:
                self.check_current_reference()
            except OSError:
                self.reference_observation = {
                    "policy": "REPORT_ONLY",
                    "status": "EVIDENCE_WRITE_FAILED",
                }
            self.stop.wait(60)

    def refresh(self, instrument_id):
        if (
            getattr(self.exchange.fee, "kind", None) == "OPERATOR_FIXED_FEE"
            and getattr(self.exchange.fee, "strategy_id", "ssr_sig") == "ssr_sig"
        ):
            with self.store.transaction() as s:
                inst = s.get(Instrument, instrument_id)
                proof = inst.mapping_evidence.get("ssr_pair_review", {}) if inst else {}
            if instrument_id not in proof.get("instrument_ids", []):
                raise RiskRejected("SSR_PAIR_REFRESH_BINDING_MISSING")
            if not getattr(self.exchange, "mock", False):
                self.clock.now = datetime.now(timezone.utc)
            from sigbot.domain import content_hash

            with self.store.transaction() as s:
                rows = [s.get(Instrument, key) for key in proof["instrument_ids"]]
                fresh = all(
                    i is not None
                    and i.quality == "HEALTHY"
                    and i.book_at is not None
                    and 0 <= (self.clock.now - utc_time(i.book_at)).total_seconds() <= 2
                    and content_hash(i.book)
                    == i.mapping_evidence.get("ssr_book_window", {}).get("book_hash")
                    and i.mapping_evidence.get("ssr_book_window", {}).get("own_list_received_at")
                    and 0
                    <= (
                        self.clock.now
                        - utc_time(i.mapping_evidence["ssr_book_window"]["own_list_received_at"])
                    ).total_seconds()
                    <= 5
                    for i in rows
                )
            if fresh:
                return  # Reuse the original receipt time, never retimestamp the book.
            return self.refresh_ssr_pair(proof["instrument_ids"])
        self.exchange.refresh_book(instrument_id)
        with self.store.transaction() as s:
            inst = s.get(Instrument, instrument_id)
            assert inst is not None and self.exchange.fee is not None
            inst.fee_per_share = self.exchange.fee.per_share
            if (
                self.exchange.approval is not None
                and inst.rules_hash == self.exchange.approval.rules_hashes.get(inst.id)
                and inst.mapping_hash == self.exchange.approval.mapping_hashes.get(inst.id)
            ):
                inst.review_until = self.exchange.approval.new_orders_until

    def refresh_ssr_pair(self, instrument_ids, *, verify_nodes=False):
        """Same account book window, shared budget and original official reader."""
        from sigbot.strategies.mm_sig_v1.data import normalized_book

        if self.exchange.approval is None or self.exchange.fee is None:
            raise AuthorizationRequired("SSR_PAIR_READ_AUTHORITY_REQUIRED")

        with self.store.transaction() as s:
            insts = [s.get(Instrument, key) for key in instrument_ids]
            scopes = []
            from sigbot.storage import Market

            for inst in insts:
                if inst is None:
                    raise SchemaMismatch("SSR_PAIR_INSTRUMENT_MISSING")
                market = s.get(Market, inst.market_id)
                if market is None:
                    raise SchemaMismatch("SSR_PAIR_MARKET_MISSING")
                scopes.append(
                    (inst.id, inst.exchange_id, market.api_id, market.raw["reviewed_sig_nodes"])
                )
        if verify_nodes:
            for _, eid, mid, expected in scopes:
                detail = self.exchange.call(
                    "market_detail",
                    path_params={"id": mid},
                    query={"tournamentId": self.exchange.tournament_id},
                )
                contexts = [
                    c
                    for c in detail["contexts"]
                    if c.get("type") == "tournament"
                    and c.get("tournament", {}).get("id") == self.exchange.tournament_id
                ]
                if (
                    detail["id"] != mid
                    or len(contexts) != 1
                    or contexts[0]["status"] != "open"
                    or eid not in {x["id"] for x in contexts[0]["exchanges"]}
                ):
                    raise RiskRejected("SSR_CURRENT_MARKET_SCOPE_CHANGED")
                nodes = self.exchange.call(
                    "market_nodes",
                    path_params={"id": mid},
                    query={"tournamentId": self.exchange.tournament_id},
                )
                if nodes != expected:
                    with self.store.transaction() as s:
                        for key in instrument_ids:
                            i = s.get(Instrument, key)
                            if i is None:
                                raise SchemaMismatch("SSR_PAIR_INSTRUMENT_MISSING")
                            self.store.audit(
                                s,
                                self.executor.account_id,
                                "OFFICIAL_RULE_DOCUMENT_CHANGE",
                                {
                                    "policy": "REPORT_ONLY",
                                    "instrument": key,
                                    "source": "market_nodes",
                                    "old": expected,
                                    "new": nodes,
                                    "old_hash": document_hash(expected),
                                    "new_hash": document_hash(nodes),
                                },
                                self.clock.now,
                            )
        before, _ = self.exchange.pages(
            "open_orders",
            query={"tournamentId": self.exchange.tournament_id, "status": "open", "limit": 200},
        )

        def read_side(scope, reader=None):
            key, eid, mid, _ = scope
            kwargs = {
                "path_params": {"id": eid},
                "query": {"tournamentId": self.exchange.tournament_id, "depth": 200},
            }
            raw = (
                self.exchange.call("orderbook", **kwargs)
                if reader is None
                else reader.call("orderbook", auth_value="Bearer " + self.exchange.key, **kwargs)
            )
            if (raw["exchangeId"], raw["marketId"]) != (eid, mid):
                raise RiskRejected("SSR_PAIR_BOOK_IDENTITY_CHANGED")
            received = (
                self.clock.now
                if getattr(self.exchange, "mock", False)
                else datetime.now(timezone.utc)
            )
            return key, normalized_book(raw, before, received, outcome="NO")

        def capture_pair(*, with_own=False):
            jobs = [None, *scopes] if with_own else scopes

            def ordered(results):
                return [*results[1:], results[0]] if with_own else results

            def capture(scope, reader=None):
                if scope is not None:
                    return read_side(scope, reader)
                kwargs = {
                    "query": {
                        "tournamentId": self.exchange.tournament_id,
                        "status": "open",
                        "limit": 200,
                    }
                }
                if reader is None:
                    own, _ = self.exchange.pages("open_orders", **kwargs)
                else:
                    own = [
                        row
                        for page in reader.iter_pages(
                            "open_orders", auth_value="Bearer " + self.exchange.key, **kwargs
                        )
                        for row in page["data"]
                    ]
                stamp = (
                    self.clock.now
                    if getattr(self.exchange, "mock", False)
                    else datetime.now(timezone.utc)
                )
                return "OWN", own, stamp

            if getattr(self.exchange, "mock", False):
                with ThreadPoolExecutor(max_workers=len(jobs)) as workers:
                    return ordered(list(workers.map(capture, jobs)))
            from sigbot.official import DocumentedReads
            from sigbot.readcheck import ReadBudget

            with self.exchange.read_budget_lock:
                budget = self.exchange.budget
                if (
                    budget.count + len(jobs) > budget.max_requests
                    or time.monotonic() >= budget.deadline
                ):
                    budget = self.exchange.budget = ReadBudget(budget.path, 80, 600)
                # A proposal must yield its read turn, not hold the account
                # loop while other readers keep the shared window above 80.
                slots = budget.reserve_gets(len(jobs), self.exchange.config.read_budget, wait=False)

            def scheduled(job):
                index, scope = job
                charged = False

                def start(category, weight):
                    nonlocal charged
                    if charged or category != "READ" or weight != 1:
                        raise AuthorizationRequired("SSR_RESERVED_GET_SINGLE_USE_REQUIRED")
                    charged = True
                    time.sleep(max(0, slots[index] - time.time()))

                reader = DocumentedReads(
                    self.exchange.config,
                    self.exchange.contract,
                    start,
                    self.exchange.pause,
                    observe=self.exchange.archive_read,
                    documented_read_session=True,
                )
                reader.schema = self.exchange.schema
                try:
                    return capture(scope, reader)
                finally:
                    reader.close()

            with ThreadPoolExecutor(max_workers=len(jobs)) as workers:
                return ordered(list(workers.map(scheduled, enumerate(jobs))))

        captured = capture_pair()
        after, _ = self.exchange.pages(
            "open_orders",
            query={"tournamentId": self.exchange.tournament_id, "status": "open", "limit": 200},
        )
        from sigbot.domain import content_hash

        if content_hash(before) != content_hash(after):
            raise RiskRejected("SSR_OWN_ORDER_WINDOW_CHANGED")
        # The ownership check can consume most of the book TTL. Take new books
        # against its newer complete list; keep every original receipt archived.
        before = after
        window = capture_pair(with_own=True)
        captured, (_, final_own, own_received) = window[:2], window[2]
        if content_hash(before) != content_hash(final_own):
            raise RiskRejected("SSR_OWN_ORDER_WINDOW_CHANGED")
        if not getattr(self.exchange, "mock", False):
            self.clock.now = datetime.now(timezone.utc)
        if (self.clock.now - own_received).total_seconds() > 5 and all(
            0 <= (self.clock.now - b.received_at).total_seconds() <= 5 for _, b in captured
        ):
            latest, _ = self.exchange.pages(
                "open_orders",
                query={"tournamentId": self.exchange.tournament_id, "status": "open", "limit": 200},
            )
            if content_hash(latest) != content_hash(final_own):
                raise RiskRejected("SSR_OWN_ORDER_WINDOW_CHANGED")
            own_received = (
                self.clock.now
                if getattr(self.exchange, "mock", False)
                else datetime.now(timezone.utc)
            )
            self.clock.sync()
        if not 0 <= (self.clock.now - own_received).total_seconds() <= 5:
            raise RiskRejected("SSR_OWN_ORDER_SNAPSHOT_STALE")
        with self.store.transaction() as s:
            for key, book in captured:
                inst = s.get(Instrument, key)
                if inst is None:
                    raise SchemaMismatch("SSR_PAIR_INSTRUMENT_MISSING")
                inst.book = book.model_dump(mode="json")
                inst.book_at, inst.quality, inst.fee_per_share = (
                    book.received_at,
                    "HEALTHY",
                    self.exchange.fee.per_share,
                )
                inst.review_until = self.exchange.approval.new_orders_until
                inst.mapping_evidence = {
                    **inst.mapping_evidence,
                    "live_checked_at": self.clock.now.isoformat(),
                    "ssr_book_window": {
                        "own_list_received_at": own_received.isoformat(),
                        "scope": "COMPLETE_STABLE_ORDER_WINDOW_THEN_FRESH_BOOKS",
                        "atomic_watermark": False,
                        "book_hash": content_hash(inst.book),
                    },
                }
                if verify_nodes:
                    inst.mapping_evidence = {
                        **inst.mapping_evidence,
                        "ssr_current_reference": {
                            "at": self.clock.now.isoformat(),
                            "binding": [inst.rules_hash, inst.mapping_hash],
                            "basis": "CURRENT_OFFICIAL_DETAIL_AND_NODES_EQUAL_REVIEW",
                        },
                    }

    def manage_ssr(self):
        family = ("ssr_sig", "1.0")
        if family not in self.authorities.items or self.clock.now < self.ssr_next:
            return
        self.ssr_next = self.clock.now + timedelta(seconds=120)
        from sigbot.strategies.ssr import SsrPlanner
        from sigbot.strategies.ssr_runtime import Parameters, SsrProducer

        results = []
        try:
            with self.activate(family):
                parameters = Parameters.model_validate(self.document.get("ssr_parameters", {}))
                planner = SsrPlanner(
                    self.store,
                    self.executor.config,
                    self.clock,
                    self.executor.planner.starts_at,
                    self.executor.planner.ends_at,
                )
                producer = SsrProducer(planner, self.executor.account_id, parameters)
            for pair in self.document["ssr_pairs"]:
                self.refresh_account_if_due()
                with self.activate(family):
                    self.refresh_ssr_pair(pair["instrument_ids"], verify_nodes=True)
                    result = producer.cycle(pair["pair_id"], pair["instrument_ids"], execute=True)
                    results.append(result)
                    if result.get("allocation", {}).get("commands"):
                        for command in result["allocation"]["commands"]:
                            self.consumer.process(command)
                        break  # One active basket; do not age its quotes scanning other races.
            self.ssr_status = {
                "status": "SCANNED",
                "at": self.clock.now.isoformat(),
                "pairs": results,
            }
        except SigbotError as exc:
            # A failed SSR proposal keeps its own reservation/gates; it does not
            # permanently disable the verified account's other strategy scopes.
            self.ssr_status = {
                "status": "BLOCKED",
                "at": self.clock.now.isoformat(),
                "code": exc.code,
                "pairs": results,
            }

    def active_orders(self):
        with self.store.transaction() as s:
            return list(
                s.scalars(select(Order).where(Order.account_id == self.executor.account_id))
            )

    def recover_orders(self):
        active = [
            o
            for o in self.active_orders()
            if o.state not in TERMINAL
            or o.request_state != "RESOLVED"
            or o.cancel_state in {"CANCEL_UNKNOWN", "CANCEL_REQUESTED"}
            or o.reserve_cash
            or o.reserve_quantity
        ]
        for order in active:
            with self.store.transaction() as s:
                row = s.get(IntentRow, order.intent_id)
                assert row is not None
                family = row.payload["strategy_id"], row.payload["strategy_version"]
            # Legacy completed strategies need no write authority for a read recovery.
            identity = (
                family if family in self.authorities.items else self.authorities.primary.identity
            )
            with self.authorities.activate(self.executor, identity):
                if order.request_state != "SEND_PENDING":
                    if order.remote_id is None and isinstance(self.exchange, SigExecutionExchange):
                        try:
                            with self.executor.account_operation(order_id=order.id):
                                self.exchange.recover_expired_submission(order.id)
                        except TransportUnknown as exc:
                            self.executor.mark_unknown(order.id, "SUBMIT_UNKNOWN")
                            # An unresolved replay is a persistent account gate,
                            # not a fatal error that stops read reconciliation.
                            self.publish(exc.code)
                    self.executor.resolve(order.id)
                    if family in self.authorities.items:
                        self.recover_pending_cancel(order.id)

    def recover_pending_cancel(self, order_id):
        """A missing legacy cleanup authority cannot stop other strategy scopes."""
        try:
            self.executor.retry_pending_cancel(order_id)
        except AuthorizationRequired as exc:
            with self.store.transaction() as session:
                self.store.audit(
                    session,
                    self.executor.account_id,
                    "CANCEL_RECOVERY_AUTHORITY_UNAVAILABLE",
                    {"order": order_id, "code": exc.code},
                    self.clock.now,
                )

    def reconcile(self):
        started_monotonic = time.monotonic()
        exchange = getattr(self, "exchange", None)
        requests_before = len(getattr(exchange, "read_receipts", []))
        started = self.clock.now
        self.last_reconciliation = self.executor.reconcile()
        self.last_reconciliation_timing = {
            "duration_seconds": time.monotonic() - started_monotonic,
            "read_requests": len(getattr(exchange, "read_receipts", [])) - requests_before,
            "completed_at": self.clock.now.isoformat(),
            **getattr(exchange, "last_account_data", {}).get("_read_metrics", {}),
        }
        self.next_reconcile = max(
            started + timedelta(seconds=15), self.clock.now + timedelta(seconds=5)
        )
        state = self.executor.controls.status(self.executor.account_id)
        parallel = bool(
            getattr(getattr(self.executor, "config", None), "parallel_strategy_execution", False)
        )
        if (
            self.last_reconciliation["status"] == "PASS"
            and self.resume_requested
            and state["reason"]
            in {
                "MANUAL_RESUME_REQUIRED",
                "MM_TRANSIENT_READ_UNAVAILABLE",
                "SNAPSHOT_WINDOW_INDETERMINATE",
                "MM_UNRESOLVED_ORDER_RECONCILIATION",
                "SUBMIT_UNKNOWN",
                "ABSENCE_NOT_PROOF_OF_NON_ACCEPTANCE",
                "EXPIRED_UNKNOWN_NO_ORDER_CONFIRMED",
                "SUBMIT_NOT_RESOLVED",
                "PRE_HTTP_REQUEST_RECOVERED",
                "RECONCILIATION_DIFFERENCE",
                "CANCEL_UNKNOWN",
                "SHARED_EXECUTOR_STOPPED",
                "SHARED_EXECUTOR:TRANSPORT_UNKNOWN",
                "SHARED_EXECUTOR:READ_RESPONSE_UNAVAILABLE",
                "SHARED_EXECUTOR:LIVE_ORDER_READ_WINDOW_CHANGED",
                "SHARED_EXECUTOR:RATE_LIMITED",
                "SHARED_EXECUTOR:SHARED_READ_BUDGET_EXHAUSTED",
                "SHARED_EXECUTOR:READ_SESSION_LIMIT_REACHED",
                "ECR_RUNTIME:READ_SESSION_LIMIT_REACHED",
                "ECR_RUNTIME:LIVE_ORDER_READ_WINDOW_CHANGED",
                "ECR_RUNTIME:SHARED_READ_BUDGET_EXHAUSTED",
                "SHARED_EXECUTOR:ACCOUNT_OPERATION_BUSY",
                "SHARED_EXECUTOR:EXPLICIT_TOURNAMENT_SCOPE_REQUIRED",
                "SHARED_EXECUTOR:READ_CHECKPOINT_CONTENT_CONFLICT",
                "SHARED_EXECUTOR:EXTERNAL_OPEN_ORDER_IMPORT_REQUIRED",
            }
            and (
                not state["unknown_count"]
                and not state.get("in_flight_count", 0)
                and not state["cancel_pending_count"]
                or parallel
                and bool(self.last_reconciliation.get("isolated_unknown_orders"))
                and all(item["isolatable"] for item in state.get("unresolved_requests", []))
            )
            and (
                state["reason"] != "RECONCILIATION_DIFFERENCE"
                or parallel
                and bool(self.last_reconciliation.get("isolated_unknown_orders"))
            )
        ):
            kwargs = {"isolate_unknowns": True} if parallel else {}
            self.executor.controls.resume(
                self.executor.account_id, self.authorities.primary.approval.operator, **kwargs
            )

    def manage_mm_risk(self):
        from sigbot.mm_risk import supervise

        if not getattr(self.authorities, "items", None):
            self.mm_risk_status = {"enabled": False, "status": "NOT_CONFIGURED"}
            return
        try:
            self.mm_risk_status = supervise(self)
        except SigbotError as exc:
            self.mm_risk_status = {
                "enabled": True,
                "status": "MM_RISK_SCOPE_BLOCKED",
                "code": exc.code,
            }

    def refresh_account_if_due(self):
        """Yield between strategy phases; keep the original financial gates."""
        self.clock.sync()
        if self.clock.now >= self.next_reconcile:
            with account_read_priority(self.executor.account_id, self.exchange.tournament_id):
                self.reconcile()
                self.publish()

    def manage_ecr_exits(self):
        family = ("ecr_sig", "1.0")
        authority = self.authorities.items.get(family)
        if authority is None:
            return
        with self.store.transaction() as s:
            active = s.scalar(
                select(StrategyEvent)
                .where(
                    StrategyEvent.account_id == self.executor.account_id,
                    StrategyEvent.strategy_id == "ecr_sig",
                    StrategyEvent.state != "CLOSED",
                )
                .limit(1)
            )
        if active is None:
            return
        with self.authorities.activate(self.executor, family):
            if self.ecr_exits is None:
                from sigbot.strategies.ecr_sig_v1.inputs import RuntimeReviews
                from sigbot.strategies.ecr_sig_v1.runtime import EcrRuntime

                reviews = RuntimeReviews.model_validate_json(
                    Path(self.document["ecr_review_input"]).read_text()
                )
                # Borrow the existing owner; never start/close a second executor.
                self.ecr_exits = EcrRuntime(
                    self.executor, reviews.account, refresh_book=self.refresh
                )
            path = Path("runtime/live/strategies/ecr_sig_v1/shared-references.local.json")
            references = {}
            if path.exists():
                data = json.loads(path.read_text())
                if (
                    data["account_id"] == self.executor.account_id
                    and 0 <= (self.clock.now - utc_time(data["at"])).total_seconds() <= 5
                ):
                    references = {k: Decimal(v) for k, v in data["references"].items()}
            result: dict = {"exit_orders": [], "exits": {}}
            self.ecr_exits.exits(references, result)

    def manage_semantic_exits(self):
        family = ("semantic_sig", "1.0")
        if family not in self.authorities.items:
            return
        from sigbot.strategies.semantic_sig_v1.live import SemanticPlanner

        with self.store.transaction() as session:
            active = list(
                session.scalars(
                    select(StrategyEvent).where(
                        StrategyEvent.account_id == self.executor.account_id,
                        StrategyEvent.strategy_id == family[0],
                        StrategyEvent.strategy_version == family[1],
                        StrategyEvent.state != "CLOSED",
                    )
                )
            )
        for event in active:
            deadline = utc_time(event.data["feature_available_at"]) + timedelta(
                seconds=event.data["horizon_seconds"]
            )
            if self.clock.now < deadline:
                continue
            try:
                with self.activate(family, "READ"):
                    from sigbot.command_queue import CommandQueue

                    queue = CommandQueue(self.store, self.executor.account_id, *family)
                    with self.store.transaction() as session:
                        buys = list(
                            session.scalars(
                                select(Order).where(
                                    Order.account_id == self.executor.account_id,
                                    Order.instrument_id == event.instrument_id,
                                    Order.side == "BUY",
                                )
                            )
                        )
                    for order in buys:
                        if order.reserve_cash and order.state not in TERMINAL:
                            queue.cancel(order.id, self.clock.now)
                    self.refresh(event.instrument_id)
                    planner = SemanticPlanner(
                        self.store,
                        self.executor.config,
                        self.clock,
                        self.executor.planner.starts_at,
                        self.executor.planner.ends_at,
                    )
                    intent = planner.exit_intent(self.executor.account_id, event.event_key)
                    if intent:
                        queue.publish(intent)
            except (
                RiskRejected,
                ContractNotVerified,
                AuthorizationRequired,
                RateLimited,
                SchemaMismatch,
                TransportUnknown,
            ) as exc:
                with self.store.transaction() as session:
                    self.store.audit(
                        session,
                        self.executor.account_id,
                        "SEMANTIC_EXIT_BLOCKED",
                        {"event_key": event.event_key, "code": exc.code},
                        self.clock.now,
                    )

    def publish(self, error=None):
        state = self.executor.controls.status(self.executor.account_id)
        # History payloads contain full quote evidence and grow without bound.
        # Status needs counts and one latest result, never every past proposal.
        from sqlalchemy import func

        by_strategy = {}
        with self.store.transaction() as s:
            counts: dict[tuple[str, str], dict[str, int]] = {}
            for strategy_id, version, command_state, count in s.execute(
                select(
                    ExecutionCommand.strategy_id,
                    ExecutionCommand.strategy_version,
                    ExecutionCommand.state,
                    func.count(),
                )
                .where(ExecutionCommand.account_id == self.executor.account_id)
                .group_by(
                    ExecutionCommand.strategy_id,
                    ExecutionCommand.strategy_version,
                    ExecutionCommand.state,
                )
            ):
                counts.setdefault((strategy_id, version), {})[command_state] = count
            for family, authority in self.authorities.items.items():
                states = counts.get(family, {})
                latest = s.execute(
                    select(ExecutionCommand.result)
                    .where(
                        ExecutionCommand.account_id == self.executor.account_id,
                        ExecutionCommand.strategy_id == family[0],
                        ExecutionCommand.strategy_version == family[1],
                    )
                    .order_by(ExecutionCommand.created_at.desc(), ExecutionCommand.id.desc())
                    .limit(1)
                ).first()
                by_strategy[":".join(family)] = {
                    "commands": sum(states.values()),
                    "waiting_allocation": states.get("WAITING_ALLOCATION", 0),
                    "OMS_pending": states.get("OMS_PENDING", 0),
                    "last_result": latest[0] if latest else None,
                    "approval_binding_current": all(
                        getattr(authority.approval, k) == v
                        for k, v in bindings(authority.config).items()
                    ),
                }
        atomic_json(
            self.output,
            {
                "at": datetime.now(timezone.utc).isoformat(),
                "role": "PARALLEL_ACCOUNT_EXECUTOR"
                if self.executor.config.parallel_strategy_execution
                else "SOLE_ACCOUNT_EXECUTOR",
                "status": "RUNNING" if state["lifecycle"] == "HEALTHY" else "PAUSED",
                "account": state,
                "execution_scope": "PARTIAL"
                if state.get("unresolved_requests")
                else "ALL_RECONCILED",
                "strategies": by_strategy,
                "reconciliation": self.last_reconciliation,
                "reconciliation_timing": getattr(self, "last_reconciliation_timing", None),
                "real_submit_attempts": self.exchange.call_count["POST"],
                "real_cancel_attempts": self.exchange.call_count["DELETE"],
                "last_error": error,
                "contract_review_error": getattr(self, "contract_review_error", None),
                "reference_observation": getattr(self, "reference_observation", None),
                "MM_RISK": getattr(self, "mm_risk_status", {"enabled": False}),
                "layer": "REAL_ACCOUNT_RUNTIME",
                "synthetic_data_used": False,
                "SSR": self.ssr_status,
                "GRID_V2": v2.status
                if (v2 := getattr(self, "grid_v2", None)) is not None
                else {"status": "NOT_REGISTERED"},
                "GRID": grid.status
                if (grid := getattr(self, "grid", None)) is not None
                else {"status": "NOT_REGISTERED"},
            },
        )

    def run(self):
        self.executor.start()
        document_worker = threading.Thread(
            target=self.document_worker, name="shared-document-observer", daemon=True
        )
        document_worker.start()
        try:
            self.recover_orders()
            if (grid := getattr(self, "grid", None)) is not None:
                grid.start()
            while not self.stop.is_set():
                self.clock.now = datetime.now(timezone.utc)
                try:
                    self.refresh_account_if_due()
                    with account_read_priority(
                        self.executor.account_id, self.exchange.tournament_id
                    ):
                        self.consumer.pending(
                            acknowledged_via_snapshot=True,
                            after_command=self.refresh_account_if_due,
                        )
                        self.manage_mm_risk()
                        self.refresh_account_if_due()
                        # Produce from the just-reconciled account immediately
                        # before allocation. A slow SSR scan must not consume
                        # a grid intent's short lifetime before its first pump.
                        if (grid := getattr(self, "grid", None)) is not None:
                            try:
                                grid.cycle()
                            except SigbotError as exc:
                                grid.status = {"status": "LOCAL_BLOCKED", "code": exc.code}
                        if (v2 := getattr(self, "grid_v2", None)) is not None:
                            try:
                                v2.cycle()
                            except SigbotError as exc:
                                v2.status = {"status": "LOCAL_BLOCKED", "code": exc.code}
                        self.consumer.pump(limit=1)
                        self.publish()  # Persist grid/account evidence before slower proposal scans.
                        self.refresh_account_if_due()
                        self.manage_ecr_exits()
                        self.refresh_account_if_due()
                        self.manage_semantic_exits()
                        self.refresh_account_if_due()
                        self.manage_ssr()
                    self.publish()
                except SigbotError as exc:
                    if exc.code != "ACCOUNT_OPERATION_BUSY":
                        self.executor.controls.pause(
                            self.executor.account_id, "SHARED_EXECUTOR:" + exc.code
                        )
                    self.publish(exc.code)
                    self.next_reconcile = self.clock.now + timedelta(seconds=10)
                self.stop.wait(1)
        finally:
            self.stop.set()
            document_worker.join(timeout=1)
            if self.executor.config.parallel_strategy_execution:
                with self.store.transaction() as s:
                    self.store.audit(
                        s,
                        self.executor.account_id,
                        "STRATEGY_WORKER_STOPPED",
                        {"role": "PARALLEL_ACCOUNT_EXECUTOR"},
                        self.clock.now,
                    )
            else:
                self.executor.controls.pause(self.executor.account_id, "SHARED_EXECUTOR_STOPPED")
            self.publish()
            self.executor.close()
            self.exchange.close()
            self.store.engine.dispose()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--settings", required=True)
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--authorize-submit", action="store_true", required=True)
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()
    runtime = None
    try:
        runtime = SharedRuntime(args.settings, args.env_file, args.resume)
        for signum in (signal.SIGINT, signal.SIGTERM):
            signal.signal(signum, lambda *_: runtime.stop.set())
        runtime.run()
    except (SigbotError, OSError, ValueError) as exc:
        print(
            json.dumps(
                {
                    "status": "FAILED",
                    "code": exc.code
                    if isinstance(exc, SigbotError)
                    else "SHARED_EXECUTOR_CONFIG_OR_STORAGE_INVALID",
                }
            )
        )
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
