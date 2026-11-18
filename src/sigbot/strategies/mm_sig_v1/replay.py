"""Finite SIM replay, shared OMS and persistent per-run store; no automatic fills."""

import json
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

from sigbot.adapters import FakeExchange
from sigbot.config import Config, RiskLimits
from sigbot.domain import Clock
from sigbot.execution import Executor
from sigbot.operations import Controls
from sigbot.reconciliation import Reconciler
from sigbot.simulation import ready, state
from sigbot.storage import Instrument, Market, Store
from sigbot.strategies.mm_sig_v1.measurements import reference, summarize
from sigbot.strategies.mm_sig_v1.models import MmConfig, Snapshot
from sigbot.strategies.mm_sig_v1.oms import MmPlanner
from sigbot.strategies.router import StrategyRouter


def setup(snapshot: Snapshot, store: Store, cfg: MmConfig):
    if (
        not snapshot.synthetic
        or snapshot.account.inventory
        or snapshot.account.reserved
        or snapshot.account.total_cost
        or snapshot.account.cash != snapshot.account.initial
    ):
        raise ValueError("MM replay requires flat SIM account; never imports a real account")
    if snapshot.account.initial != cfg.initial_capital:
        raise ValueError("MM capital mismatch")
    clock = Clock(snapshot.now)
    fake = FakeExchange(clock, snapshot.account.cash, snapshot.account_id, snapshot.tournament_id)
    with store.transaction() as s:
        for c in snapshot.candidates:
            if not s.get(Market, c.market_id):
                s.add(
                    Market(
                        id=c.market_id,
                        tournament=snapshot.tournament_id,
                        api_id=c.market_id,
                        title="Synthetic MM event",
                        category="OTHER",
                        source="SYNTHETIC",
                        rules_hash=c.rules_hash,
                        raw={},
                    )
                )
                s.flush()
            data = c.model_dump(mode="json")
            s.add(
                Instrument(
                    id=c.instrument_id,
                    exchange_id=c.exchange_id,
                    market_id=c.market_id,
                    tournament=snapshot.tournament_id,
                    event_group=c.event_key,
                    outcome="YES",
                    rules_hash=c.rules_hash,
                    mapping_hash=c.mapping_hash,
                    tick=c.tick,
                    step=c.step,
                    fee_per_share=c.fee_per_share,
                    reviewed=c.reviewed,
                    review_until=c.review_until,
                    status=c.status,
                    quality=c.book.quality,
                    book_at=c.book.received_at,
                    book=c.book.model_dump(mode="json", exclude={"received_at", "quality"}),
                    mapping_evidence={"source": "SYNTHETIC", "mm_candidate": data},
                )
            )
    Controls(store, clock).initialize(fake.snapshot())
    app = Config(
        risk=RiskLimits(
            max_order_notional=cfg.max_order_cost,
            max_total_exposure=cfg.max_total_cost,
            max_event_exposure=cfg.max_event_cost,
            max_instrument_quantity=cfg.max_inventory,
            max_open_orders=cfg.max_events * 2,
        )
    )
    router = StrategyRouter(store, app, clock, snapshot.starts_at, snapshot.ends_at)
    planner = MmPlanner(store, app, clock, snapshot.starts_at, snapshot.ends_at)
    executor = Executor(store, app, clock, fake, router, fake.account_id)
    ready(executor)
    return clock, fake, app, planner, executor


def replay(path: str, cfg: MmConfig) -> dict:
    packet = json.loads(Path(path).read_text())
    snapshot = Snapshot.model_validate(packet["snapshot"])
    run_id = uuid4().hex
    folder = Path("runtime/offline/strategies/mm_sig_v1/runs") / run_id
    folder.mkdir(parents=True, exist_ok=False)
    store = Store("sqlite+pysqlite:///" + str(folder / "oms.db"))
    store.create_for_mock()
    clock, fake, app, planner, executor = setup(snapshot, store, cfg)
    instrument_id = snapshot.candidates[0].instrument_id
    orders, trace = {}, []
    try:
        for ev in packet["events"]:
            clock.advance(ev.get("seconds", 0))
            kind = ev["type"]
            if kind == "QUOTE":
                orders = planner.reserve_quotes(fake.account_id, instrument_id, cfg, ev["round_id"])
                for oid in orders.values():
                    if oid is not None:
                        executor.dispatch(oid)
            elif kind == "FILL":
                oid = orders[ev["side"]]
                obs = fake.fill(
                    oid,
                    ev["fill_id"],
                    Decimal(ev["quantity"]),
                    Decimal(ev["price"]),
                    Decimal(ev["fee"]),
                )
                executor.observe(oid, obs)
                executor.observe(oid, obs)
            elif kind in {"CANCEL", "CANCEL_TIMEOUT"}:
                if kind == "CANCEL_TIMEOUT":
                    fake.faults.append("cancel_timeout")
                executor.cancel(orders[ev["side"]])
            elif kind == "BOOK":
                with store.transaction() as s:
                    inst = s.get(Instrument, instrument_id)
                    assert inst is not None
                    book = dict(inst.book)
                    book["source_at"] = clock.now.isoformat()
                    book["sequence"] += 1
                    inst.book, inst.book_at = book, clock.now
                    inst.mapping_evidence = {
                        **inst.mapping_evidence,
                        "mm_candidate": {
                            **inst.mapping_evidence["mm_candidate"],
                            "activity_until": clock.now.isoformat(),
                        },
                    }
            elif kind == "REFERENCE":
                from sigbot.strategies.mm_sig_v1.oms import candidate

                with store.transaction() as s:
                    inst = s.get(Instrument, instrument_id)
                    assert inst is not None
                    ref_book = candidate(inst).book
                reference(store, clock, fake.account_id, instrument_id, ref_book, cfg)
            elif kind == "TOUCH":
                pass  # A book touching a quote provides no evidence of our execution.
            elif kind == "RESTART":
                executor.close()
                executor = Executor(store, app, clock, fake, executor.planner, fake.account_id)
                ready(executor)
            else:
                raise ValueError("unsupported MM synthetic event")
            trace.append({"event": kind, **state(store, fake.account_id, instrument_id)})
        reconciliation = Reconciler(executor).run(fake.snapshot())
        metrics = summarize(store, clock, fake.account_id)
        if reconciliation["status"] != "PASS":
            raise AssertionError("MM reconciliation failed")
        for key, expected in packet.get("expected", {}).items():
            if metrics[key] != expected:
                raise AssertionError({"field": key, "actual": metrics[key], "expected": expected})
        return {
            "layer": "MOCK",
            "status": "PASS",
            "run_id": run_id,
            "database": str(folder / "oms.db"),
            "metrics": metrics,
            "trace": trace,
            "reconciliation": reconciliation,
            "submit_calls": fake.submit_calls,
            "cancel_calls": fake.cancel_calls,
            "real_network_calls": 0,
            "real_orders": 0,
            "execution": "SHARED_OMS_FAKE_EXCHANGE; EXPLICIT_SYNTHETIC_FILLS_ONLY",
        }
    finally:
        executor.close()
        store.engine.dispose()
