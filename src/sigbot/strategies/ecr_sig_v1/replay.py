"""Persisted multi-event FakeExchange replay through the common OMS/ledger."""

from __future__ import annotations

import json
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

from sqlalchemy import select

from sigbot.adapters import FakeExchange
from sigbot.config import Config, RiskLimits
from sigbot.domain import ZERO, Clock, utc_time
from sigbot.errors import RiskRejected
from sigbot.execution import Executor
from sigbot.operations import Controls
from sigbot.reconciliation import Reconciler
from sigbot.simulation import ready
from sigbot.storage import Account, Instrument, Market, Order, Position, Store, StrategyEvent
from sigbot.strategies.ecr_sig_v1.inputs import Snapshot
from sigbot.strategies.ecr_sig_v1.oms import EcrPlanner, entry_intent, proposal_from_json


def setup(
    snapshot: Snapshot, store: Store
) -> tuple[Clock, FakeExchange, Config, EcrPlanner, Executor]:
    if not snapshot.synthetic or snapshot.account.cash != snapshot.account.initial:
        raise RiskRejected("ECR_REPLAY_REQUIRES_SYNTHETIC_EMPTY_ACCOUNT")
    clock = Clock(snapshot.now)
    fake = FakeExchange(
        clock, snapshot.account.initial, snapshot.account_id, snapshot.tournament_id
    )
    with store.transaction() as s:
        for raw in snapshot.candidates:
            candidate = raw.candidate(snapshot.now, True)
            s.add(
                Market(
                    id=raw.market_id,
                    tournament=snapshot.tournament_id,
                    page_id=None,
                    api_id=raw.market_id,
                    title=raw.key,
                    category=raw.event.office,
                    source="SYNTHETIC",
                    rules_hash=raw.rules_current_hash,
                    raw=raw.event.model_dump(mode="json"),
                )
            )
            s.flush()
            s.add(
                Instrument(
                    id=raw.instrument_id,
                    exchange_id=raw.exchange_id,
                    market_id=raw.market_id,
                    tournament=snapshot.tournament_id,
                    event_group=candidate.event_key,
                    outcome=raw.side,
                    rules_hash=raw.rules_current_hash,
                    mapping_hash=raw.review.mapping_hash,
                    mapping_evidence={
                        "source": "SYNTHETIC",
                        "review": raw.review.model_dump(mode="json"),
                    },
                    tick=raw.tick,
                    step=raw.lot,
                    fee_per_share=raw.review.buy_fee_bound,
                    reviewed=candidate.rules_reviewed and candidate.mapping_reviewed,
                    review_until=raw.review.expires_at,
                    status="OPEN",
                    quality="HEALTHY",
                    book_at=raw.sig_book.verified_at,
                    book={
                        "own_orders_known": True,
                        "bids": [
                            {"price": str(x.price), "quantity": str(x.size), "own": False}
                            for x in raw.sig_book.bids
                        ],
                        "asks": [
                            {"price": str(x.price), "quantity": str(x.size), "own": False}
                            for x in raw.sig_book.asks
                        ],
                    },
                )
            )
    Controls(store, clock).initialize(fake.snapshot())
    initial = snapshot.account.initial
    cfg = Config(
        expected_account_id=snapshot.account_id,
        expected_tournament_id=snapshot.tournament_id,
        allowed_instruments=[x.instrument_id for x in snapshot.candidates],
        book_ttl_seconds=5,
        risk=RiskLimits(
            max_order_notional=initial * Decimal("0.01"),
            max_total_exposure=initial * Decimal("0.30"),
            max_event_exposure=initial * Decimal("0.03"),
            max_instrument_quantity=Decimal("100000000"),
            max_open_orders=12,
        ),
    )
    planner = EcrPlanner(
        store, cfg, clock, clock.now - timedelta(days=1), utc_time("2026-11-04T17:00:00Z")
    )
    executor = Executor(store, cfg, clock, fake, planner, snapshot.account_id)
    ready(executor)
    return clock, fake, cfg, planner, executor


def replay(
    path: str, runtime_root: Path = Path("runtime/offline/strategies/ecr_sig_v1/runs")
) -> dict:
    snapshot = Snapshot.model_validate_json(Path(path).read_text())
    if not snapshot.synthetic:
        raise RiskRejected("ECR_REAL_REPLAY_FORBIDDEN")
    planned = snapshot.plan()
    if len(planned["plans"]) < 3:
        raise RiskRejected("ECR_THREE_EVENT_DEMO_REQUIRES_THREE_ELIGIBLE_EVENTS")
    run_id = uuid4().hex
    directory = runtime_root / run_id
    directory.mkdir(parents=True, mode=0o700)
    database = directory / "state.db"
    store = Store("sqlite+pysqlite:///" + str(database.resolve()))
    store.create_for_mock()
    clock, fake, cfg, planner, executor = setup(snapshot, store)
    accepted = []
    trace = []
    try:
        for data in planned["plans"]:
            proposal = proposal_from_json(data)
            candidate = next(x for x in snapshot.candidates if x.key == proposal.key)
            target = entry_intent(snapshot, candidate, proposal)
            oid = planner.reserve(target)
            assert oid is not None
            assert planner.reserve(target) == oid
            accepted.append((oid, proposal, candidate))
        for oid, _, _ in accepted:
            executor.dispatch(oid)
        clock.advance(30)
        for i, (oid, proposal, _candidate) in enumerate(accepted):
            quantity = core_quantity = proposal.quantity
            if i == 0:
                quantity = (core_quantity / 2).to_integral_value()
            obs = fake.fill(oid, "SIM-ECR-ENTRY-FILL-" + str(i), quantity, proposal.limit_price)
            executor.observe(oid, obs)
            executor.observe(oid, obs)
            if quantity < proposal.quantity:
                executor.cancel(oid)
            with store.transaction() as s:
                event = planner.event(s, snapshot.account_id, proposal.event_key)
                assert event is not None and event.holding_started_at == clock.now
                trace.append(
                    {
                        "event_key": proposal.event_key,
                        "stage": "HOLDING",
                        "quantity": str(quantity),
                        "plan_created_at": snapshot.now.isoformat(),
                        "holding_started_at": event.holding_started_at.isoformat(),
                    }
                )
        executor.close()
        store.engine.dispose()
        store = Store("sqlite+pysqlite:///" + str(database.resolve()))
        planner = EcrPlanner(
            store, cfg, clock, clock.now - timedelta(days=1), utc_time("2026-11-04T17:00:00Z")
        )
        executor = Executor(store, cfg, clock, fake, planner, snapshot.account_id)
        ready(executor)
        before_exit = Reconciler(executor).run(fake.snapshot())
        assert before_exit["status"] == "PASS"
        exit_children = 0
        for _, proposal, candidate in accepted:
            for child in range(10):
                with store.transaction() as s:
                    event = planner.event(s, snapshot.account_id, proposal.event_key)
                    assert event is not None
                    if event.state == "CLOSED":
                        break
                    assert event.holding_started_at == snapshot.now + timedelta(seconds=30)
                    inst = s.get(Instrument, candidate.instrument_id)
                    assert inst is not None
                    inst.book_at = clock.now
                    inst.book = {
                        "own_orders_known": True,
                        "bids": [{"price": "0.46", "quantity": "25000", "own": False}],
                        "asks": [{"price": "0.47", "quantity": "25000", "own": False}],
                    }
                reason, exit_target = planner.exit_intent(proposal.event_key, Decimal("0.52"))
                assert reason == "TAKE_PROFIT" and exit_target is not None
                oid = planner.reserve(exit_target)
                assert oid is not None
                executor.dispatch(oid)
                with store.transaction() as s:
                    order = s.get(Order, oid)
                    assert order is not None
                    qty = order.quantity
                observation = fake.fill(
                    oid, f"SIM-ECR-EXIT-FILL-{proposal.key}-{child}", qty, Decimal("0.46")
                )
                executor.observe(oid, observation)
                executor.observe(oid, observation)
                exit_children += 1
                clock.advance(1)
            else:
                raise AssertionError("synthetic exit child bound exhausted")
        reconciliation = Reconciler(executor).run(fake.snapshot())
        with store.transaction() as s:
            account = s.get(Account, snapshot.account_id)
            assert account is not None
            states = list(
                s.scalars(select(StrategyEvent).where(StrategyEvent.account_id == account.id))
            )
            remaining = sum(
                (
                    p.quantity
                    for p in s.scalars(select(Position).where(Position.account_id == account.id))
                ),
                ZERO,
            )
            future_fees = sum((e.future_fee_reserve for e in states), ZERO)
            assert remaining == future_fees == ZERO and all(e.state == "CLOSED" for e in states)
            result = {
                **planned,
                "status": "PASS" if reconciliation["status"] == "PASS" else "FAIL",
                "run_id": run_id,
                "persistent_database": str(database),
                "account_namespace": "offline",
                "submitted_fake_orders": fake.submit_calls,
                "fake_cancel_calls": fake.cancel_calls,
                "accepted_events": len(accepted),
                "exit_child_orders": exit_children,
                "holding_clock_from_fill": True,
                "restart_preserved_state": True,
                "duplicate_fills_ignored": True,
                "cash_after": str(account.cash),
                "realized_pnl_synthetic_only": str(account.cash - snapshot.account.initial),
                "remaining_quantity": str(remaining),
                "remaining_future_fee_reserve": str(future_fees),
                "trace": trace,
                "reconciliation": reconciliation,
                "production_execution": "NOT_PERFORMED",
                "profitability_backtest": "NOT_PERFORMED",
                "real_orders": 0,
                "network_calls": 0,
            }
        (directory / "result.json").write_text(
            json.dumps(result, default=str, ensure_ascii=False, indent=2) + "\n"
        )
        return result
    finally:
        executor.close()
        store.engine.dispose()
