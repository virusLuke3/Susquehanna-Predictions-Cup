from __future__ import annotations

import json
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory

from sqlalchemy import select

from sigbot.adapters import FakeExchange
from sigbot.config import Config, RiskLimits
from sigbot.domain import ZERO, Clock, Intent, content_hash, utc_time
from sigbot.execution import Executor
from sigbot.operations import Controls
from sigbot.reconciliation import Reconciler
from sigbot.risk import Planner
from sigbot.storage import Account, Instrument, Market, Order, Position, Store


def synthetic_config() -> Config:
    return Config(
        risk=RiskLimits(
            max_order_notional=Decimal("500"),
            max_total_exposure=Decimal("1000"),
            max_event_exposure=Decimal("500"),
            max_instrument_quantity=Decimal("1000"),
            max_open_orders=10,
        )
    )


def setup(
    store: Store, clock: Clock, exchange: FakeExchange, instrument_id: str = "SIM-EVENT-A-YES"
) -> tuple[Config, Planner, Executor]:
    if not instrument_id.startswith("SIM-"):
        raise ValueError("synthetic setup requires SIM instrument")
    rules_hash, mapping_hash = content_hash("SYNTHETIC_RULES"), content_hash("SYNTHETIC_MAPPING")
    with store.transaction() as s:
        if not s.get(Market, "SIM-MARKET-A"):
            s.add(
                Market(
                    id="SIM-MARKET-A",
                    tournament=exchange.tournament_id,
                    api_id="SIM-API-MARKET-A",
                    title="Synthetic event A",
                    category="OTHER",
                    source="SYNTHETIC",
                    raw={},
                    rules_hash=rules_hash,
                )
            )
            s.flush()
        if not s.get(Instrument, instrument_id):
            s.add(
                Instrument(
                    id=instrument_id,
                    exchange_id="SIM-EXCHANGE-" + instrument_id,
                    mapping_evidence={"source": "SYNTHETIC", "protocol": "LOCAL_D_ONLY"},
                    market_id="SIM-MARKET-A",
                    tournament=exchange.tournament_id,
                    event_group="SIM-EVENT-A",
                    outcome="YES",
                    rules_hash=rules_hash,
                    mapping_hash=mapping_hash,
                    tick=Decimal("0.01"),
                    step=Decimal("1"),
                    fee_per_share=ZERO,
                    reviewed=True,
                    review_until=clock.now + timedelta(days=1),
                    status="OPEN",
                    quality="HEALTHY",
                    book_at=clock.now,
                    book={
                        "own_orders_known": True,
                        "asks": [{"price": "0.40", "quantity": "1000", "own": False}],
                        "bids": [{"price": "0.40", "quantity": "1000", "own": False}],
                    },
                )
            )
    Controls(store, clock).initialize(exchange.snapshot())
    cfg = synthetic_config()
    planner = Planner(
        store, cfg, clock, clock.now - timedelta(hours=1), clock.now + timedelta(days=1)
    )
    executor = Executor(store, cfg, clock, exchange, planner, exchange.account_id)
    return cfg, planner, executor


def intent(clock: Clock, instrument_id: str = "SIM-EVENT-A-YES", **overrides) -> Intent:
    fields = dict(
        schema_version="1.0",
        intent_id="SIM-INTENT-1",
        synthetic=True,
        account_id="SIM-ACCOUNT",
        tournament_id="SIM-CUP",
        instrument_id=instrument_id,
        outcome_side="YES",
        action="INCREASE_TO_TARGET",
        target_quantity="100",
        limit_price="0.42",
        max_order_notional="42",
        created_at=clock.now,
        expires_at=clock.now + timedelta(minutes=5),
        rules_hash=content_hash("SYNTHETIC_RULES"),
        mapping_hash=content_hash("SYNTHETIC_MAPPING"),
        strategy_id="manual_target",
        strategy_version="1",
    )
    fields.update(overrides)
    return Intent.model_validate(fields)


def ready(executor: Executor) -> None:
    executor.start()
    result = Reconciler(executor).run(executor.exchange.snapshot())
    if result["status"] != "PASS":
        raise AssertionError(result)
    executor.controls.resume(executor.account_id, "SYNTHETIC_TEST_OPERATOR")


def state(
    store: Store, account_id: str = "SIM-ACCOUNT", instrument_id: str = "SIM-EVENT-A-YES"
) -> dict:
    with store.transaction() as s:
        a = s.get(Account, account_id)
        p = s.get(Position, (account_id, instrument_id))
        assert a is not None
        orders = list(s.scalars(select(Order).where(Order.account_id == account_id)))
        qty, cost = (p.quantity, p.cost) if p else (ZERO, ZERO)
        return {
            "cash": str(a.cash),
            "position_quantity": str(qty),
            "cost_basis": str(cost),
            "average_cost": str(cost / qty if qty and cost is not None else ZERO),
            "active_reservation": str(sum((o.reserve_cash for o in orders), ZERO)),
            "active_order_quantity": str(
                sum(
                    (o.quantity - o.filled for o in orders if o.reserve_cash or o.reserve_quantity),
                    ZERO,
                )
            ),
        }


def run_cancel_race(
    store: Store, fixture: dict, restart_each: bool = False
) -> tuple[dict, Executor]:
    if fixture.get("synthetic") is not True or not fixture["instrument_id"].startswith("SIM-"):
        raise ValueError("synthetic fixture required")
    clock = Clock(utc_time("2026-10-03T00:00:00Z"))
    exchange = FakeExchange(
        clock, Decimal(fixture["initial_cash"]), account_id=fixture["account_id"]
    )
    _, planner, executor = setup(store, clock, exchange, fixture["instrument_id"])
    try:
        ready(executor)
        target = intent(
            clock,
            fixture["instrument_id"],
            target_quantity=fixture["requested_quantity"],
            limit_price=fixture["limit_price"],
        )
        order_id = None
        trace = []
        for ev in fixture["events"]:
            kind = ev["type"]
            if kind == "INTENT_RESERVED":
                order_id = planner.reserve(target)
            elif kind == "ORDER_ACCEPTED":
                assert order_id is not None
                executor.dispatch(order_id)
            elif kind == "FILL":
                assert order_id is not None
                obs = exchange.fill(
                    order_id, ev["fill_id"], Decimal(ev["quantity"]), Decimal(ev["price"])
                )
                executor.observe(order_id, obs)
                executor.observe(order_id, obs)  # Duplicate push/REST delivery.
            elif kind == "CANCEL_REQUESTED":
                assert order_id is not None
                exchange.faults.append("cancel_timeout")
                executor.cancel(order_id)
            elif kind == "CANCEL_CONFIRMED":
                assert order_id is not None
                executor.cancel(order_id)
            else:
                raise ValueError("unsupported synthetic event")
            trace.append({"event": kind, **state(store)})
            if restart_each:
                executor.close()
                executor = Executor(
                    store, executor.config, clock, exchange, planner, exchange.account_id
                )
                executor.start()
                result = Reconciler(executor).run(exchange.snapshot())
                if result["status"] == "PASS":
                    executor.controls.resume(exchange.account_id, "SYNTHETIC_RESTART_OPERATOR")
        result = state(store)
        for key, expected in fixture["expected_after_cancel"].items():
            if Decimal(result[key]) != Decimal(expected):
                raise AssertionError({"field": key, "actual": result[key], "expected": expected})
        reconciliation = Reconciler(executor).run(exchange.snapshot())
        if reconciliation["status"] != "PASS":
            raise AssertionError(reconciliation)
        return {
            "after_cancel": result,
            "trace": trace,
            "reconciliation": reconciliation,
            "submit_calls": exchange.submit_calls,
            "real_network_calls": 0,
        }, executor
    except BaseException:
        executor.close()  # Resource cleanup only; preserve and re-raise every failure.
        raise


def replay(path: str) -> dict:
    fixture = json.loads(Path(path).read_text())
    branches = []
    with TemporaryDirectory(prefix="sigbot-replay-") as d:
        store = Store("sqlite+pysqlite:///" + d + "/main.db")
        store.create_for_mock()
        result, e = run_cancel_race(store, fixture, restart_each=True)
        e.close()
        store.engine.dispose()
        # Independent databases and exchange state for every settlement branch.
        for idx, branch in enumerate(fixture["independent_settlement_branches"]):
            store = Store("sqlite+pysqlite:///" + d + f"/branch{idx}.db")
            store.create_for_mock()
            _, e = run_cancel_race(store, fixture)
            try:
                assert isinstance(e.exchange, FakeExchange)
                tx = e.exchange.settle(
                    fixture["instrument_id"],
                    "SIM-SETTLE",
                    branch["outcome"],
                    Decimal(branch["fixture_refundable_cost"])
                    if "fixture_refundable_cost" in branch
                    else None,
                )
                e.ledger.settlement(
                    e.account_id,
                    tx["id"],
                    tx["instrument_id"],
                    Decimal(tx["quantity"]),
                    Decimal(tx["cash"]),
                    Decimal(tx["cost"]),
                )
                if Decimal(state(store)["cash"]) != Decimal(branch["cash"]):
                    raise AssertionError("settlement mismatch")
                assert Reconciler(e).run(e.exchange.snapshot())["status"] == "PASS"
                branches.append(
                    {"outcome": branch["outcome"], "cash": state(store)["cash"], "status": "PASS"}
                )
            finally:
                e.close()
                store.engine.dispose()
    return {
        "layer": "MOCK",
        "status": "PASS",
        **result,
        "independent_settlement_branches": branches,
    }
