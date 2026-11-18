"""Finite operator-invoked ECR automatic runtime; no service installation."""

from __future__ import annotations

import json
import queue
import signal
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from sigbot.config import Config
from sigbot.domain import ZERO, Clock, Mode, content_hash, utc_time
from sigbot.errors import (
    AuthorizationRequired,
    Conflict,
    RateLimited,
    SchemaMismatch,
    SigbotError,
    StorageUnavailable,
)
from sigbot.execution import Executor
from sigbot.operations import Controls
from sigbot.readcheck import private_write
from sigbot.reconciliation import Reconciler
from sigbot.storage import Account, Instrument, Position, Store, StrategyEvent
from sigbot.strategies.ecr_sig_v1.inputs import RuntimeReviews, Snapshot
from sigbot.strategies.ecr_sig_v1.runtime import EcrRuntime
from sigbot.strategies.router import StrategyRouter


def accepted_fee(config: Config, approval, account_id, tournament_id):
    if not config.live_fee_file:
        return None
    from sigbot.strategies.mm_sig_v1.live.fees import EmpiricalFee, load_fee

    fee = load_fee(config.live_fee_file)
    fee.check(False)
    if not isinstance(fee, EmpiricalFee):
        return fee
    if not approval.accept_empirical_fee_model:
        raise AuthorizationRequired("EMPIRICAL_FEE_MODEL_REQUIRES_EXPLICIT_OPERATOR_ACCEPTANCE")
    evidence = json.loads(Path(fee.evidence_path).read_text())
    if (evidence.get("account_id_hash"), evidence.get("tournament_id")) != (
        content_hash(account_id),
        tournament_id,
    ):
        raise AuthorizationRequired("EMPIRICAL_FEE_ACCOUNT_OR_TOURNAMENT_MISMATCH")
    return fee


def apply_observer_fee(observer, config: Config, approval):
    if (
        not approval.operator_approved
        or not (approval.issued_at <= datetime.now(timezone.utc) < approval.new_orders_until)
        or (approval.account_id, approval.tournament_id)
        != (observer.reads.reader.config.expected_account_id, observer.tid)
    ):
        raise AuthorizationRequired("OBSERVER_FEE_ACCEPTANCE_SCOPE_OR_WINDOW_INVALID")
    fee = accepted_fee(config, approval, approval.account_id, approval.tournament_id)
    if fee is None:
        raise AuthorizationRequired("OBSERVER_CONFIGURED_FEE_REQUIRED")
    empirical = getattr(fee, "kind", None) == "EMPIRICAL_ACTUAL_TRADE_CALIBRATION"
    observer.state["fee"].update(
        status="EMPIRICAL_ACCEPTED" if empirical else "VERIFIED",
        model_kind=getattr(fee, "kind", "OFFICIAL_FIXED_RATE"),
        accepted_by_operator=approval.accept_empirical_fee_model,
        buy_fee_bound=str(fee.per_share),
        sell_fee_bound=str(fee.per_share),
        official_future_fee_schedule_verified=not empirical,
        evidence_sha256=fee.sha256,
        reason="Actual-trade estimate; future official fee schedule unverified."
        if empirical
        else "Reviewed fixed fee evidence.",
    )


def apply_empirical_fee(reviews: RuntimeReviews, config: Config, approval) -> None:
    """Use the accepted account calibration without declaring an official fee rule."""
    fee = accepted_fee(config, approval, reviews.account_id, reviews.tournament_id)
    if getattr(fee, "kind", None) != "EMPIRICAL_ACTUAL_TRADE_CALIBRATION":
        return
    for candidate in reviews.candidates:
        candidate.review.fee_verified = False
        candidate.review.fee_evidence = None
        candidate.review.empirical_fee_file = config.live_fee_file
        candidate.review.buy_fee_bound = candidate.review.sell_fee_bound = fee.per_share


def offline_run(path, root=Path("runtime/offline/strategies/ecr_sig_v1/automatic")):
    from sigbot.strategies.ecr_sig_v1.replay import setup

    seed = Snapshot.model_validate_json(Path(path).read_text())
    if not seed.synthetic:
        raise AuthorizationRequired("AUTOMATIC_OFFLINE_REQUIRES_SYNTHETIC_INPUT")
    folder = root / uuid4().hex
    store = Store("sqlite+pysqlite:///" + str((folder / "state.db").resolve()))
    store.create_for_mock()
    clock, fake, config, policy, executor = setup(seed, store)
    executor.close()
    executor.planner = StrategyRouter(store, config, clock, policy.starts_at, policy.ends_at)
    runtime = EcrRuntime(executor, seed.account)
    trace = []
    try:
        runtime.start(resume=True, operator="SIM-OPERATOR")
        trace.append(runtime.tick(seed))
        for _ in range(10):
            clock.advance(1)
            # Explicitly controlled FakeExchange fills, not a real-book
            # touch-implies-fill model and not a profit/alpha validation.
            for oid, observation in list(fake.orders.items()):
                if observation.state in {"OPEN", "PARTIAL"}:
                    request = fake.requests[oid]
                    fake.fill(
                        oid,
                        "SIM-AUTO-FILL-" + oid,
                        request.quantity - observation.filled,
                        request.price,
                    )
            with store.transaction() as s:
                for instrument in s.scalars(select(Instrument)):
                    instrument.book_at = clock.now
                    instrument.book = {
                        "own_orders_known": True,
                        "bids": [{"price": ".46", "quantity": "25000", "own": False}],
                        "asks": [{"price": ".47", "quantity": "25000", "own": False}],
                    }
            trace.append(
                runtime.tick(
                    references={c.event.event_key(True): Decimal(".52") for c in seed.candidates}
                )
            )
            with store.transaction() as s:
                events = list(s.scalars(select(StrategyEvent)))
                if events and all(e.state == "CLOSED" for e in events):
                    break
        reconciliation = Reconciler(executor).run(fake.snapshot())
        with store.transaction() as s:
            events = list(s.scalars(select(StrategyEvent)))
            quantity = sum((p.quantity for p in s.scalars(select(Position))), ZERO)
        passed = (
            bool(events)
            and all(e.state == "CLOSED" for e in events)
            and quantity == ZERO
            and reconciliation["status"] == "PASS"
        )
        result = {
            "strategy_id": "ecr_sig",
            "strategy_version": "1.0",
            "layer": "MOCK",
            "status": "PASS_AUTOMATIC_LIFECYCLE" if passed else "FAIL",
            "accepted_events": len(events),
            "fake_submit_calls": fake.submit_calls,
            "cash_after": str(fake.cash),
            "remaining_quantity": str(quantity),
            "real_orders": 0,
            "real_cancels": 0,
            "network_calls": 0,
            "fill_model": "CONTROLLED_SYNTHETIC_FAKEEXCHANGE",
            "profitability_verified": False,
            "reconciliation": reconciliation,
            "trace": trace,
            "database": str(folder / "state.db"),
        }
        private_write(folder / "result.json", json.dumps(result).encode())
        return result, 0 if passed else 4
    finally:
        runtime.close()
        store.engine.dispose()


def shared_idle_review_status(runtime, candidate_count):
    """An empty idle strategy observes the shared account without duplicating REST work."""
    if candidate_count or not runtime.config.parallel_strategy_execution or not runtime.idle():
        return None
    account = runtime.executor.controls.status(runtime.account_id)
    stamp = account.get("reconciled_at")
    age = (runtime.clock.now - utc_time(stamp)).total_seconds() if stamp else None
    return {
        "at": runtime.clock.now.isoformat(),
        "status": "WAITING_VERIFIED_MARKET_REVIEW",
        "layer": "REAL_READ_SHARED_ACCOUNT",
        "account": account,
        "account_observation": "SHARED_PERSISTED_ACCOUNT_STATE_NOT_NEW_HTTP_SNAPSHOT",
        "account_observation_current": age is not None
        and 0 <= age <= runtime.config.account_ttl_seconds
        and account["lifecycle"] == "HEALTHY"
        and not account.get("unknown_count")
        and not account.get("cancel_pending_count"),
        "qualified_candidates": 0,
        "entry_orders": [],
        "exit_orders": [],
        "real_submit_attempts": runtime.executor.exchange.call_count["POST"],
        "real_cancel_attempts": runtime.executor.exchange.call_count["DELETE"],
        "review_input_candidates": 0,
        "approved_execution_review_cards": 0,
        "execution_admission_status": "WAITING_VERIFIED_MARKET_REVIEW",
    }


def run(args, config: Config):
    if getattr(args, "inspect", False):
        from sigbot.live import bindings

        raw_document = Path(args.input).read_text()
        reviews = (
            RuntimeReviews.from_snapshot(Snapshot.model_validate_json(raw_document))
            if "now" in json.loads(raw_document)
            else RuntimeReviews.model_validate_json(raw_document)
        )
        blockers = []
        if config.mode != Mode.LIVE_LIMITED:
            blockers.append("LIVE_LIMITED_CONFIG_NOT_SELECTED")
        if not config.live_fee_file:
            blockers.append("EXECUTION_FEE_EVIDENCE_NOT_CONFIGURED")
        for c in reviews.candidates:
            if not c.review.approved or not all(m.approved for m in c.review.mappings):
                blockers.append("REVIEW_NOT_APPROVED:" + c.key)
        return {
            "status": "LOCAL_INSPECTION_ONLY",
            "strategy_id": "ecr_sig",
            "layer": "STATIC",
            "bindings": bindings(config),
            "rules_hashes": {c.instrument_id: c.rules_current_hash for c in reviews.candidates},
            "mapping_hashes": {c.instrument_id: c.review.mapping_hash for c in reviews.candidates},
            "local_blockers": blockers,
            "credentials_loaded": False,
            "network_calls": 0,
            "real_orders": 0,
            "live_authorized": False,
        }, 0
    if args.duration_seconds <= 0:
        raise AuthorizationRequired("AUTOMATIC_RUN_MUST_HAVE_POSITIVE_FINITE_DURATION")
    if config.mode == Mode.OFFLINE:
        if config.business_network_enabled:
            raise AuthorizationRequired("OFFLINE_NETWORK_FORBIDDEN")
        result, code = offline_run(args.input)
        if args.output:
            private_write(Path(args.output), json.dumps(result).encode())
        return result, code
    if config.mode != Mode.LIVE_LIMITED or not args.authorize_read or not args.authorize_submit:
        raise AuthorizationRequired("FINITE_LIVE_LIMITED_READ_AND_SUBMIT_AUTHORIZATION_REQUIRED")
    if not args.approval_file:
        raise AuthorizationRequired("OPERATOR_MAINTAINED_APPROVAL_FILE_REQUIRED")
    deadline = time.monotonic() + args.duration_seconds
    run_until = datetime.now(timezone.utc) + timedelta(seconds=args.duration_seconds)
    from sigbot.authorization import Approval
    from sigbot.live import SigExecutionExchange, bindings, stage_ecr
    from sigbot.readcheck import load_key
    from sigbot.strategies.ecr_sig_v1.observer import atomic_json, run_observer
    from sigbot.strategies.ecr_sig_v1.runtime_inputs import ReviewedFeed

    raw_input = Path(args.input).read_text()
    template = (
        RuntimeReviews.from_snapshot(Snapshot.model_validate_json(raw_input))
        if "now" in json.loads(raw_input)
        else RuntimeReviews.model_validate_json(raw_input)
    )
    if template.synthetic or (template.account_id, template.tournament_id) != (
        config.expected_account_id,
        config.expected_tournament_id,
    ):
        raise AuthorizationRequired("AUTOMATIC_LIVE_TEMPLATE_SCOPE_MISMATCH")
    approval = Approval.model_validate_json(Path(args.approval_file).read_text())
    if any(getattr(approval, k) != v for k, v in bindings(config).items()):
        raise AuthorizationRequired("APPROVAL_BINDING_CHANGED")
    if "SUBMIT" not in approval.allowed_actions:
        raise AuthorizationRequired("AUTOMATIC_SUBMIT_SCOPE_REQUIRED")
    apply_empirical_fee(template, config, approval)
    store = Store(config.database_url, "live")
    exchange, runtime = None, None
    stop = threading.Event()
    mailbox: queue.Queue = queue.Queue(maxsize=1)
    pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="sig-ecr-sole-executor")
    future = None
    feed = ReviewedFeed(template)
    output = Path(args.output or "runtime/live/strategies/ecr_sig_v1/automatic-status.json")
    approved_count = sum(
        c.review.approved and bool(c.review.mappings) and all(m.approved for m in c.review.mappings)
        for c in template.candidates
    )
    try:
        with store.transaction() as s:
            for raw in template.candidates:
                old = s.get(Instrument, raw.instrument_id)
                if old and (old.exchange_id, old.tournament, old.outcome, old.event_group) != (
                    raw.exchange_id,
                    template.tournament_id,
                    raw.side,
                    raw.event.event_key(False),
                ):
                    raise Conflict("AUTOMATIC_CANNOT_REMAP_EXISTING_INSTRUMENT")
        stage_ecr(store, template, reviews_only=True, now=datetime.now(timezone.utc))
        clock = Clock()
        exchange = SigExecutionExchange(store, config, clock, load_key(args.env_file), approval)
        exchange.execution_until = run_until
        exchange.authorized()
        cup = exchange.call(
            "tournament_identity", path_params={"slug": config.expected_tournament_slug}
        )
        if cup["id"] != template.tournament_id:
            raise AuthorizationRequired("AUTOMATIC_TOURNAMENT_SCOPE_MISMATCH")
        with store.transaction() as s:
            initialized = s.get(Account, template.account_id) is not None
        if not initialized:
            Controls(store, clock).initialize_live(exchange.snapshot())
        planner = StrategyRouter(
            store, config, clock, utc_time(cup["startDate"]), utc_time(cup["endDate"])
        )
        executor = Executor(store, config, clock, exchange, planner, template.account_id)
        runtime = EcrRuntime(
            executor, template.account, refresh_book=exchange.refresh_book, run_until=run_until
        )

        def worker():
            while not stop.is_set() and time.monotonic() < deadline:
                clock.now = datetime.now(timezone.utc)
                idle = shared_idle_review_status(runtime, len(template.candidates))
                if idle is not None:
                    runtime.last_result = idle
                    atomic_json(output, idle)
                    stop.wait(5)
                    continue
                try:
                    runtime.start(resume=args.resume, operator=approval.operator)
                    break
                except RateLimited as exc:
                    if exc.code != "ACCOUNT_OPERATION_BUSY":
                        raise
                    stop.wait(1)
            while not stop.is_set() and time.monotonic() < deadline:
                try:
                    frame = mailbox.get(timeout=5)
                except queue.Empty:
                    frame = None
                if stop.is_set() or time.monotonic() >= deadline:
                    break
                clock.now = datetime.now(timezone.utc)
                result = runtime.tick(
                    frame, references=lambda: feed.references(store, template.account_id, clock.now)
                )
                result["feed_rejections"] = dict(feed.rejected)
                result["review_input_candidates"] = len(template.candidates)
                result["approved_execution_review_cards"] = approved_count
                result["execution_admission_status"] = (
                    "RECHECK_ALL_CONDITIONS_BEFORE_ORDER"
                    if approved_count
                    else "WAITING_VERIFIED_MARKET_REVIEW"
                )
                atomic_json(output, result)

        def started(observer):
            nonlocal future
            if (observer.tid, observer.reads.reader.config.expected_account_id) != (
                template.tournament_id,
                template.account_id,
            ):
                raise AuthorizationRequired("AUTOMATIC_OBSERVER_TOURNAMENT_MISMATCH")
            observer.state.update(
                normal_live_bridge_enabled=True,
                approved_execution_review_cards=approved_count,
                execution_admission_status="RECHECK_ALL_CONDITIONS_BEFORE_ORDER"
                if approved_count
                else "WAITING_VERIFIED_MARKET_REVIEW",
            )
            apply_observer_fee(observer, config, approval)
            future = pool.submit(worker)

        def cycle(observer):
            if future is not None and future.done():
                future.result()
            frame = feed.snapshot(observer, datetime.now(timezone.utc))
            # Keep only an unprocessed latest frame. Old timestamps are retained
            # and the allocator rejects stale frames; no backlog replay buying.
            try:
                mailbox.get_nowait()
            except queue.Empty:
                pass
            mailbox.put_nowait(frame)

        from types import SimpleNamespace

        remaining = int(deadline - time.monotonic())
        if remaining <= 0:
            raise AuthorizationRequired("AUTOMATIC_RUN_DEADLINE_REACHED")
        bounded = SimpleNamespace(
            **{
                **vars(args),
                "duration_seconds": remaining,
                "observer_root": output.parent / "observation",
                "observer_report": output.parent / "observation-status.json",
            }
        )
        if not template.candidates and runtime.idle():
            # The independent observer keeps collecting; an empty execution
            # review needs no second universe of books and cannot submit orders.
            previous = {
                s: signal.signal(s, lambda *_: stop.set()) for s in (signal.SIGTERM, signal.SIGINT)
            }
            try:
                future = pool.submit(worker)
                while time.monotonic() < deadline and not stop.wait(1):
                    if future.done():
                        future.result()
            finally:
                for signum, handler in previous.items():
                    signal.signal(signum, handler)
        else:
            run_observer(
                bounded,
                config.contract_file,
                on_started=started,
                on_cycle=cycle,
                fee_config=config,
            )
        stop.set()
        pool.shutdown(wait=True, cancel_futures=True)
        if future is not None:
            future.result()
        last = runtime.last_result
        runtime.close()
        result = {
            "status": "STOPPED",
            "last_cycle": last,
            "account": executor.controls.status(template.account_id),
            "output": str(output),
            "service_started": False,
        }
        atomic_json(output, result)
        return result, 0 if last.get("status") in {
            "RUNNING",
            "WAITING_VERIFIED_MARKET_REVIEW",
        } else 6
    except SigbotError as exc:
        if exchange:
            exc.execution_attempts = dict(exchange.call_count)
        raise
    except (OSError, ValueError, SQLAlchemyError) as exc:
        failure = (
            StorageUnavailable("AUTOMATIC_STORAGE_UNAVAILABLE")
            if isinstance(exc, SQLAlchemyError)
            else SchemaMismatch("AUTOMATIC_INPUT_OR_STORAGE_INVALID")
        )
        if exchange:
            failure.execution_attempts = dict(exchange.call_count)
        raise failure from None
    finally:
        try:
            stop.set()
            pool.shutdown(wait=True, cancel_futures=True)
            try:
                if runtime:
                    runtime.close()
            finally:
                try:
                    if exchange:
                        exchange.close()
                finally:
                    store.engine.dispose()
        except (OSError, SQLAlchemyError):
            failure = StorageUnavailable("AUTOMATIC_CLEANUP_STORAGE_UNAVAILABLE")
            if exchange:
                failure.execution_attempts = dict(exchange.call_count)
            raise failure from None
