"""Persisted three-event synthetic acceptance through the sole shared OMS."""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

from sqlalchemy import select

from sigbot.adapters import FakeExchange
from sigbot.config import Config, RiskLimits
from sigbot.domain import ZERO, Clock
from sigbot.errors import RiskRejected
from sigbot.execution import Executor
from sigbot.operations import Controls
from sigbot.reconciliation import Reconciler
from sigbot.simulation import ready
from sigbot.storage import (
    Account,
    Instrument,
    Market,
    Order,
    Position,
    Store,
    StrategyDayRisk,
    StrategyEvaluation,
    StrategyEvent,
)
from sigbot.strategies.prc_sig_v1.models import POLICY, Snapshot
from sigbot.strategies.prc_sig_v1.oms import PrcPlanner, read_plan
from sigbot.strategies.prc_sig_v1.polls import PollRepository
from sigbot.strategies.router import StrategyRouter


def setup(snapshot, store):
    if not snapshot.synthetic:
        raise RiskRejected("PRC_REPLAY_REQUIRES_SYNTHETIC")
    clock = Clock(snapshot.now)
    fake = FakeExchange(
        clock, snapshot.account.initial, snapshot.account_id, snapshot.tournament_id
    )
    with store.transaction() as s:
        for raw in snapshot.candidates:
            if s.get(Market, raw.api_market_id):
                continue
            s.add(
                Market(
                    id=raw.api_market_id,
                    tournament=snapshot.tournament_id,
                    page_id=None,
                    api_id=raw.api_market_id,
                    title=raw.key,
                    category=raw.event.office,
                    source="SYNTHETIC",
                    rules_hash=raw.rules_current_hash,
                    raw=raw.event.model_dump(mode="json"),
                )
            )
            s.flush()
            for side, iid in raw.instrument_ids.items():
                book = raw.books[side]
                s.add(
                    Instrument(
                        id=iid,
                        exchange_id=raw.exchange_id,
                        market_id=raw.api_market_id,
                        tournament=snapshot.tournament_id,
                        event_group=raw.event.event_key(True),
                        outcome=side,
                        rules_hash=raw.rules_current_hash,
                        mapping_hash=raw.mapping_hash,
                        mapping_evidence={
                            "source": "SYNTHETIC",
                            "review": raw.review.model_dump(mode="json"),
                        },
                        tick=raw.tick,
                        step=raw.lot,
                        fee_per_share=raw.review.buy_fee_bound,
                        reviewed=raw.review.approved,
                        review_until=raw.review.expires_at,
                        status="OPEN",
                        quality="HEALTHY",
                        book_at=book.verified_at,
                        book={
                            "source_at": book.source_at.isoformat(),
                            "version": book.version,
                            "own_orders_known": True,
                            "bids": [x.model_dump(mode="json") for x in book.bids],
                            "asks": [x.model_dump(mode="json") for x in book.asks],
                        },
                    )
                )
        repo = PollRepository(store)
        for report in snapshot.reports:
            repo.ingest_locked(s, report)
    Controls(store, clock).initialize(fake.snapshot())
    initial = snapshot.account.initial
    cfg = Config(
        expected_account_id=snapshot.account_id,
        expected_tournament_id=snapshot.tournament_id,
        book_ttl_seconds=5,
        risk=RiskLimits(
            max_order_notional=initial * POLICY.single_fraction,
            max_total_exposure=initial * POLICY.total_fraction,
            max_event_exposure=initial * POLICY.single_fraction,
            max_instrument_quantity=Decimal("100000000"),
            max_open_orders=3,
        ),
    )
    start, end = clock.now - timedelta(days=1), clock.now + timedelta(days=20)
    planner = PrcPlanner(store, cfg, clock, start, end)
    router = StrategyRouter(store, cfg, clock, start, end)
    executor = Executor(store, cfg, clock, fake, router, snapshot.account_id)
    ready(executor)
    return clock, fake, cfg, planner, executor


def replay(path, runtime_root=Path("runtime/offline/strategies/prc_sig_v1/runs")):
    snapshot = Snapshot.model_validate_json(Path(path).read_text())
    directory = runtime_root / uuid4().hex
    directory.mkdir(parents=True, mode=0o700)
    database = directory / "state.db"
    store = Store("sqlite+pysqlite:///" + str(database.resolve()))
    store.create_for_mock()
    clock, fake, cfg, planner, executor = setup(snapshot, store)
    trace = []
    try:
        allocated = planner.allocate_and_reserve(snapshot)
        if len(allocated["plans"]) != 3:
            raise RiskRejected("PRC_THREE_EVENT_REPLAY_REQUIRES_THREE_PLANS")
        for raw in allocated["plans"]:
            executor.dispatch(raw["order_id"])
        clock.advance(30)
        for i, raw in enumerate(allocated["plans"]):
            p = read_plan({k: v for k, v in raw.items() if k != "order_id"})
            quantity = p.quantity / 2 if i == 0 else p.quantity
            with store.transaction() as s:
                inst = s.get(Instrument, p.instrument_id)
                assert inst is not None
                inst.book_at = clock.now
                inst.book = {**inst.book, "source_at": clock.now.isoformat()}
            obs = fake.fill(raw["order_id"], f"SIM-PRC-ENTRY-{i}", quantity, p.limit_price)
            executor.observe(raw["order_id"], obs)
            executor.observe(raw["order_id"], obs)
            if quantity < p.quantity:
                executor.cancel(raw["order_id"])
            trace.append(
                {
                    "event": p.event_key,
                    "office": snapshot.candidates[i].event.office,
                    "actual_first_fill": clock.now.isoformat(),
                    "filled_quantity": str(quantity),
                }
            )
        executor.close()
        store.engine.dispose()
        store = Store("sqlite+pysqlite:///" + str(database.resolve()))
        router = StrategyRouter(
            store, cfg, clock, clock.now - timedelta(days=1), clock.now + timedelta(days=20)
        )
        planner = PrcPlanner(store, cfg, clock, router.starts_at, router.ends_at)
        executor = Executor(store, cfg, clock, fake, router, snapshot.account_id)
        ready(executor)
        assert Reconciler(executor).run(fake.snapshot())["status"] == "PASS"
        exit_children = 0
        for raw in allocated["plans"]:
            for child in range(3):
                with store.transaction() as s:
                    event = planner.event(s, snapshot.account_id, raw["event_key"])
                    if event.state == "CLOSED":
                        break
                    assert event.holding_started_at == snapshot.now + timedelta(seconds=30)
                    inst = s.get(Instrument, event.instrument_id)
                    assert inst is not None
                    inst.book_at = clock.now
                    inst.book = {
                        "own_orders_known": True,
                        "bids": [{"price": "0.54", "quantity": "100000"}],
                        "asks": [{"price": "0.55", "quantity": "100000"}],
                    }
                reason, target = planner.exit_intent(raw["event_key"])
                assert reason == "TAKE_PROFIT" and target is not None
                oid = executor.planner.reserve(target)
                assert oid is not None
                executor.dispatch(oid)
                with store.transaction() as s:
                    order = s.get(Order, oid)
                    assert order is not None
                    qty = order.quantity
                obs = fake.fill(oid, f"SIM-PRC-EXIT-{raw['key']}-{child}", qty, Decimal("0.54"))
                executor.observe(oid, obs)
                executor.observe(oid, obs)
                exit_children += 1
                clock.advance(1)
            with store.transaction() as s:
                assert planner.event(s, snapshot.account_id, raw["event_key"]).state == "CLOSED"
        reconciliation = Reconciler(executor).run(fake.snapshot())
        with store.transaction() as s:
            account = s.get(Account, snapshot.account_id)
            assert account is not None
            assert all(p.quantity == 0 and p.cost == 0 for p in s.scalars(select(Position)))
            events = list(s.scalars(select(StrategyEvent)))
            assert all(e.state == "CLOSED" and e.future_fee_reserve == 0 for e in events)
            latches = list(s.scalars(select(StrategyEvaluation)))
            daily = sum((d.committed for d in s.scalars(select(StrategyDayRisk))), ZERO)
            assert len(latches) == 3 and daily == Decimal("1500")
            result = {
                **allocated,
                "layer": "MOCK",
                "status": "PASS" if reconciliation["status"] == "PASS" else "FAIL",
                "validation_scope": "SYNTHETIC_OMS_AND_SQLITE_RESTART_ONLY",
                "persistent_database": str(database.resolve()),
                "accepted_events": 3,
                "submitted_fake_orders": fake.submit_calls,
                "fake_cancel_calls": fake.cancel_calls,
                "exit_children": exit_children,
                "cash_after": str(account.cash),
                "daily_committed_not_recycled": str(daily),
                "permanent_release_event_latches": len(latches),
                "states": [e.state for e in events],
                "trace": trace,
                "reconciliation": reconciliation,
                "synthetic_pnl": str(account.cash - snapshot.account.initial),
                "real_orders": 0,
                "network_calls": 0,
                "real_performance_validated": False,
            }
        return result
    finally:
        executor.close()
        store.engine.dispose()
