"""Grid producer and official history sampler for the existing shared gateway."""

import json
import os
import threading
from collections import Counter
from datetime import datetime, timedelta, timezone
from decimal import ROUND_FLOOR, Decimal
from pathlib import Path
from typing import Any

from sqlalchemy import select

from sigbot.command_queue import CommandQueue
from sigbot.domain import ZERO, Intent, content_hash, utc_time
from sigbot.errors import RiskRejected, SigbotError
from sigbot.history import HistoryReads, candles, stamp
from sigbot.readcheck import load_key
from sigbot.shared_marketdata import account_priority_active
from sigbot.storage import (
    ExecutionCommand,
    Instrument,
    IntentRow,
    Market,
    Order,
    Position,
    StrategyEvent,
)
from sigbot.strategies.ecr_sig_v1.observer import atomic_json
from sigbot.strategies.grid_sig import (
    IDENTITY,
    Parameters,
    ceil_tick,
    finances,
    floor_tick,
    own_orders,
    owned_lots,
    profile,
)

D = Decimal


def now():
    return datetime.now(timezone.utc)


class HistorySampler:
    """Reuse official sparse candle pagination and the workspace-wide GET budget."""

    def __init__(self, store, config, root, env_file, stop, parameters=None):
        self.store, self.config = store, config
        self.root, self.env_file, self.stop = Path(root), env_file, stop
        self.parameters = parameters or Parameters()
        self.thread = None

    def catalog(self):
        with self.store.transaction() as session:
            return sorted(
                (i.exchange_id, m.api_id)
                for i, m in session.execute(
                    select(Instrument, Market)
                    .join(Market, Instrument.market_id == Market.id)
                    .where(
                        Instrument.tournament == self.config.expected_tournament_id,
                        Instrument.outcome == "YES",
                    )
                )
                if i.exchange_id and m.api_id
            )

    def fetch(self, reader, exchange, market):
        at = now()
        end = at.replace(minute=at.minute // 5 * 5, second=0, microsecond=0)
        body = candles(
            reader,
            market,
            exchange,
            self.config.expected_tournament_id,
            # 48h contains at most 577 five-minute buckets including the left
            # margin, below the existing 1000-row page limit. Keep longer
            # descriptive windows without changing the live 6h entry policy.
            end - timedelta(hours=48, minutes=5),
            end,
            resolution="5m",
        )
        available = now()
        path = self.root / "history" / f"{exchange}.json"
        previous = json.loads(path.read_text()) if path.exists() else {}
        if previous and (previous.get("exchange_id"), previous.get("market_id")) != (exchange, market):
            raise RiskRejected("GRID_HISTORY_CACHE_IDENTITY_MISMATCH")
        known = {r["end"]: r for r in previous.get("rows", [])}
        rows = [
            {
                "end": (stamp(c["time"]) + timedelta(minutes=5)).isoformat(),
                "mean": str(c["vwap"]),
                "available_at": available.isoformat(),
            }
            for c in body["candles"]
        ]
        for row in rows:
            prior = known.get(row["end"])
            if prior and D(prior["mean"]) == D(row["mean"]):
                row["available_at"] = prior["available_at"]
        result = {
            "exchange_id": exchange,
            "market_id": market,
            "received_at": available.isoformat(),
            "window_complete": body["window_complete"],
            "requested_from": (end - timedelta(hours=48, minutes=5)).isoformat(),
            "requested_to": end.isoformat(),
            "rows": rows,
            "receipts": list(reader.requests[-body["pages"] :]),
        }
        # Replace only after the entire official window passes identity/paging.
        atomic_json(path, result)
        return result

    def reader(self):
        reader = HistoryReads(
            load_key(self.env_file),
            tournament_slug=self.config.expected_tournament_slug,
            root=Path("runtime/readonly"),
            contract_file=self.config.contract_file,
            max_requests=1000000,
            seconds=86400,
            minute_limit=60,  # Own history cap, within the unchanged common 100-GET cap.
        )
        reader.reader.config.expected_account_id = self.config.expected_account_id
        reader.reader.config.expected_tournament_id = self.config.expected_tournament_id
        reader.reader.config.expected_tournament_slug = self.config.expected_tournament_slug
        return reader

    def run(self, *, once=False):
        reader = self.reader()
        counts: Counter[str] = Counter()
        errors: dict[str, dict] = {}
        try:
            while not self.stop.is_set():
                sweep_started = now()
                for exchange, market in self.catalog():
                    source = os.environ.get("SIGBOT_SHARED_SIG_DATA")
                    while source and account_priority_active(
                        Path(source).with_suffix(".account-read.json"),
                        self.config.expected_account_id,
                        self.config.expected_tournament_id,
                        now(),
                    ):
                        if self.stop.wait(0.2):
                            break
                    if self.stop.is_set():
                        break
                    try:
                        self.fetch(reader, exchange, market)
                        counts["received"] += 1
                        errors.pop(exchange, None)
                    except SigbotError as exc:
                        errors[exchange] = {"code": exc.code, "at": now().isoformat()}
                        counts["failed"] += 1
                    atomic_json(
                        self.root / "HISTORY_STATUS.json",
                        {
                            "at": now().isoformat(),
                            "layer": "REAL_READ",
                            "counts": dict(counts),
                            "errors": errors,
                            "current_exchange": exchange,
                            "sweep_started_at": sweep_started.isoformat(),
                            "history_minute_limit": 60,
                            "shared_minute_limit": 100,
                            "real_orders": 0,
                            "real_cancels": 0,
                        },
                    )
                    # The common rolling limiter paces requests. A second
                    # per-market sleep made the catalog stale without reserving
                    # actual capacity for account recovery.
                if once or self.stop.wait(5):
                    break
                reader.reader.close()
                reader.key = ""
                reader = self.reader()  # Renew the finite session, retain the shared rate budget.
        finally:
            reader.reader.close()
            reader.key = ""

    def start(self):
        self.thread = threading.Thread(target=self.run, name="grid-official-history", daemon=True)
        self.thread.start()


class GridRunner:
    identity = IDENTITY
    parameter_class = Parameters

    def __init__(self, runtime, settings, env_file):
        self.runtime, self.store = runtime, runtime.store
        self.settings = settings
        self.root = Path(settings["output"])
        self.entries_disabled = settings.get("entries_disabled", False)
        self.parameters = self.parameter_class.model_validate(settings.get("parameters", {}))
        self.queue = CommandQueue(self.store, runtime.executor.account_id, *self.identity)
        self.config = runtime.authorities.items[self.identity].config
        allocated = self.config.strategy_capital_allocations.get(self.identity[0])
        if allocated is not None and allocated != self.parameters.strategy_cash:
            raise RiskRejected("GRID_CAPITAL_CONFIGURATION_MISMATCH")
        self.sampler = HistorySampler(
            self.store, self.config, self.root, env_file, runtime.stop, self.parameters
        )
        self.next_scan = now()
        self.status: dict[str, Any] = {"status": "STARTING", "realized_pnl": "0"}

    def start(self):
        self.sampler.start()

    def make_intent(self, session, inst, quantity, price, note):
        timestamp = now()
        from sigbot.storage import Position

        account_id = self.runtime.executor.account_id
        pos = session.get(Position, (account_id, inst.id))
        held = pos.quantity if pos else ZERO
        buy = note["kind"] == "ENTRY"
        synthetic = self.store.namespace in {"offline", "shadow"}
        return Intent(
            schema_version="1.0",
            intent_id=("SIM-GRID-" if synthetic else "SIG-GRID-")
            + content_hash(
                [inst.id, note, timestamp.isoformat(), str(held), str(quantity), str(price)]
            ),
            synthetic=synthetic,
            account_id=account_id,
            tournament_id=self.config.expected_tournament_id,
            instrument_id=inst.id,
            outcome_side=inst.outcome,
            action="INCREASE_TO_TARGET" if buy else "REDUCE_TO_TARGET",
            target_quantity=held + quantity if buy else held - quantity,
            limit_price=price,
            max_order_notional=max(self.parameters.order_cash, quantity * price),
            created_at=timestamp,
            expires_at=timestamp + timedelta(seconds=30),
            rules_hash=inst.rules_hash,
            mapping_hash=inst.mapping_hash,
            strategy_id=self.identity[0],
            strategy_version=self.identity[1],
            note=json.dumps(note),
        )

    def history(self, exchange, timestamp):
        path = self.root / "history" / f"{exchange}.json"
        if not path.exists():
            return {"eligible": False, "reason": "HISTORY_PENDING"}
        document = json.loads(path.read_text())
        if document["exchange_id"] != exchange or not document["window_complete"]:
            return {"eligible": False, "reason": "HISTORY_SCOPE_OR_PAGING_GAP"}
        try:
            return profile(document["rows"], timestamp, self.parameters)
        except SigbotError as exc:
            return {"eligible": False, "reason": exc.code}

    def cycle(self):
        timestamp = now()
        if timestamp < self.next_scan:
            return
        self.next_scan = timestamp + timedelta(seconds=10)
        account_id = self.runtime.executor.account_id
        # Stops and regime exits must observe real current books even when the
        # MM producer no longer selects an exchange owned by this strategy.
        with self.store.transaction() as session:
            tracked = [
                session.get(Instrument, e.instrument_id)
                for e in session.scalars(
                    select(StrategyEvent).where(
                        StrategyEvent.account_id == account_id,
                        StrategyEvent.strategy_id == self.identity[0],
                        StrategyEvent.strategy_version == self.identity[1],
                        StrategyEvent.state != "CLOSED",
                    )
                )
            ]
            oldest = min(
                tracked,
                key=lambda i: utc_time(i.book.get("validated_at") or i.book_at),
                default=None,
            )
        if oldest:
            with self.runtime.activate(self.identity, "READ"):
                self.runtime.refresh(oldest.id)
            timestamp = now()
        candidates: list[tuple[Decimal | int, Intent]] = []
        cancels: list[str] = []
        reasons: Counter[str] = Counter()
        profiles: list[dict] = []
        p = self.parameters
        entries_paused = self.entries_disabled or (self.root / "PAUSE_NEW_ENTRIES").exists()
        with self.store.transaction() as session:
            self.store.lock_account(session, account_id)
            all_inst = list(
                session.scalars(
                    select(Instrument).where(
                        Instrument.tournament == self.config.expected_tournament_id
                    )
                )
            )
            by_exchange: dict[str, dict[str, Instrument]] = {}
            for inst in all_inst:
                by_exchange.setdefault(inst.exchange_id, {})[inst.outcome] = inst
            active = own_orders(session, account_id, active=True)
            inst_by_id = {i.id: i for i in all_inst}
            own_assets = {lot.instrument_id for lot, _ in owned_lots(session, account_id)}
            protected = set()
            for pos in session.scalars(
                select(Position).where(Position.account_id == account_id, Position.quantity > ZERO)
            ):
                if pos.instrument_id not in own_assets:
                    protected.add(inst_by_id[pos.instrument_id].exchange_id)
            for owner in session.scalars(
                select(StrategyEvent).where(
                    StrategyEvent.account_id == account_id,
                    StrategyEvent.strategy_id != self.identity[0],
                    StrategyEvent.state != "CLOSED",
                )
            ):
                for key in [owner.instrument_id, *owner.data.get("instrument_ids", [])]:
                    if key in inst_by_id:
                        protected.add(inst_by_id[key].exchange_id)
            for order, origin in session.execute(
                select(Order, IntentRow)
                .join(IntentRow, Order.intent_id == IntentRow.id)
                .where(
                    Order.account_id == account_id,
                    (Order.reserve_cash > ZERO) | (Order.reserve_quantity > ZERO),
                )
            ):
                if (
                    origin.payload.get("strategy_id"),
                    origin.payload.get("strategy_version"),
                ) != self.identity:
                    protected.add(inst_by_id[order.instrument_id].exchange_id)
            rejected = set(
                session.scalars(
                    select(ExecutionCommand.instrument_id).where(
                        ExecutionCommand.account_id == account_id,
                        ExecutionCommand.strategy_id == self.identity[0],
                        ExecutionCommand.state == "REJECTED_BEFORE_ALLOCATION",
                        ExecutionCommand.created_at > timestamp - timedelta(seconds=60),
                    )
                )
            )
            queued = list(
                session.scalars(
                    select(ExecutionCommand).where(
                        ExecutionCommand.account_id == account_id,
                        ExecutionCommand.strategy_id == self.identity[0],
                        ExecutionCommand.strategy_version == self.identity[1],
                        ExecutionCommand.state == "WAITING_ALLOCATION",
                    )
                )
            )
            for command in queued:
                if utc_time(command.expires_at) <= timestamp:
                    if command.order_id is not None:
                        raise RiskRejected("GRID_WAITING_COMMAND_HAS_OMS_ORDER")
                    command.state, command.result = (
                        "EXPIRED_BEFORE_ALLOCATION",
                        {"code": "COMMAND_EXPIRED"},
                    )
            queued = [c for c in queued if c.state == "WAITING_ALLOCATION"]
            waiting = {c.instrument_id for c in queued if utc_time(c.expires_at) > timestamp}
            funds = finances(session, account_id, p)
            for exchange, directions in sorted(by_exchange.items()):
                f = self.history(exchange, timestamp)
                profiles.append({"exchange_id": exchange, **f})
                if exchange in protected:
                    reasons["FOREIGN_ASSET_OR_RISK_SCOPE_PROTECTED"] += 1
                    continue
                event = session.get(StrategyEvent, (account_id, *self.identity, exchange))
                working = [
                    (o, r)
                    for o, r in active
                    if o.instrument_id in {i.id for i in directions.values()}
                ]
                current = event is not None and event.state != "CLOSED"
                if current:
                    inst = session.get(Instrument, event.instrument_id)
                elif f.get("eligible"):
                    side = "YES" if D(f["last"]) <= D(f["mu"]) else "NO"
                    inst = directions.get(side)
                else:
                    inst = None
                if inst is None:
                    reasons[f.get("reason") or "DIRECTION_MAPPING_MISSING"] += 1
                    continue
                if inst.id in rejected:
                    reasons["LOCAL_REJECTION_BACKOFF"] += 1
                    continue
                lots = owned_lots(session, account_id, inst.id)
                frozen_mu = (
                    D(event.data["mu"])
                    if current
                    else (D(f["mu"]) if inst.outcome == "YES" else 1 - D(f["mu"]))
                )
                width = D(event.data["width"]) if current else D(f["width"])
                live_mu = (
                    None
                    if not f.get("mu")
                    else (D(f["mu"]) if inst.outcome == "YES" else 1 - D(f["mu"]))
                )
                risk = event.data.get("risk_exit") if current else None
                if (
                    lots
                    and event.holding_started_at
                    and (timestamp - utc_time(event.holding_started_at)).total_seconds()
                    >= p.holding_seconds
                ):
                    risk = "TIMEOUT"
                if (
                    current
                    and live_mu is not None
                    and abs(live_mu - frozen_mu) > max(D(".01"), 2 * width)
                ):
                    risk = "REGIME_BREAK"
                if lots and not f.get("eligible"):
                    risk = "HISTORY_GAP"
                # Stops use only current official bids; missing books cannot
                # invent an exit. Native allocation refreshes again before send.
                bids = [
                    (D(x["price"]), D(x["quantity"]) - D(x.get("own_quantity", "0")))
                    for x in inst.book.get("bids", [])
                    if not x.get("own")
                ]
                bids = sorted([(a, q) for a, q in bids if q > 0], reverse=True)
                book_current = bool(
                    inst.book.get("validated_at")
                    and 0
                    <= (timestamp - utc_time(inst.book["validated_at"])).total_seconds()
                    <= self.config.book_ttl_seconds
                )
                if (
                    lots
                    and bids
                    and book_current
                    and bids[0][0] <= lots[0][0].cost / lots[0][0].quantity - p.stop_distance
                ):
                    risk = "STOP"
                if risk and event:
                    event.data = {**event.data, "risk_exit": risk}
                for order, origin in working:
                    expired = (
                        timestamp - utc_time(origin.payload["created_at"])
                    ).total_seconds() >= p.order_seconds
                    stale = order.side == "BUY" and (entries_paused or not f.get("eligible"))
                    if expired or stale or risk:
                        if not order.cancel_state:
                            cancels.append(order.id)
                if inst.id in waiting:
                    reasons["COMMAND_PENDING"] += 1
                    continue
                note = {
                    "exchange_id": exchange,
                    "mu": str(frozen_mu),
                    "width": str(width),
                    "parameters": p.model_dump(mode="json"),
                }
                if risk:
                    if working:
                        reasons["RISK_WAIT_CANCEL_CONFIRMATION"] += 1
                    elif lots and bids:
                        quantity = min(
                            sum((lot.quantity for lot, _ in lots), ZERO), bids[0][1]
                        ).to_integral_value(rounding=ROUND_FLOOR)
                        if quantity:
                            candidates.append(
                                (
                                    0,
                                    self.make_intent(
                                        session,
                                        inst,
                                        quantity,
                                        bids[0][0],
                                        {**note, "kind": "RISK_EXIT", "reason": risk},
                                    ),
                                )
                            )
                    else:
                        reasons["RISK_EXIT_BOOK_PENDING"] += 1
                    continue
                if lots and not any(o.side == "SELL" for o, _ in working):
                    lot, frozen = lots[0]
                    target = max(
                        D(frozen["target"]),
                        ceil_tick(lot.cost / lot.quantity + inst.tick, inst.tick),
                    )
                    if target < 1:
                        candidates.append(
                            (
                                1,
                                self.make_intent(
                                    session,
                                    inst,
                                    lot.quantity,
                                    target,
                                    {
                                        **note,
                                        "kind": "EXIT",
                                        "lot_id": lot.id,
                                        "target": str(target),
                                    },
                                ),
                            )
                        )
                    continue
                if not f.get("eligible"):
                    reasons[f.get("reason", "PROFILE_UNAVAILABLE")] += 1
                    continue
                if entries_paused:
                    reasons["NEW_ENTRIES_PAUSED_EXIT_MANAGEMENT_ACTIVE"] += 1
                    continue
                if any(o.side == "BUY" for o, _ in working):
                    reasons["BUY_RESTING"] += 1
                    continue
                drift = D(f["drift"]) if f.get("drift") is not None else ZERO
                if (drift if inst.outcome == "YES" else -drift) <= D("-.01"):
                    reasons["ADVERSE_DRIFT"] += 1
                    continue
                occupied = {n["slot"] for _, n in lots}
                slot = next((j for j in range(1, 4) if j not in occupied), None)
                if slot is None or funds["available"] <= 0:
                    reasons["INVENTORY_OR_CASH_LIMIT"] += 1
                    continue
                level = frozen_mu / (1 + width / frozen_mu) ** slot
                price = floor_tick(level, inst.tick)
                target = min(D(".995"), ceil_tick(min(frozen_mu, level + width), inst.tick))
                if price <= 0 or target - price < inst.tick:
                    reasons["GRID_BELOW_TICK_OR_NO_EDGE"] += 1
                    continue
                budget = min(
                    p.order_cash,
                    funds["available"],
                    p.market_cash - sum((lot.cost for lot, _ in lots), ZERO),
                )
                quantity = max(ZERO, (budget / price).to_integral_value(rounding=ROUND_FLOOR))
                if quantity:
                    latest = D(f["last"]) if inst.outcome == "YES" else 1 - D(f["last"])
                    candidates.append(
                        (
                            2 + abs(latest - level),
                            self.make_intent(
                                session,
                                inst,
                                quantity,
                                price,
                                {
                                    **note,
                                    "kind": "ENTRY",
                                    "slot": slot,
                                    "profile": f,
                                    "level": str(level),
                                    "target": str(target),
                                },
                            ),
                        )
                    )
            self.status = {
                "at": timestamp.isoformat(),
                "status": "SCANNING",
                "strategy": list(self.identity),
                "market_count": len(by_exchange),
                "profile_eligible": sum(f.get("eligible", False) for f in profiles),
                "candidate_count": len(candidates),
                "active_orders": len(active),
                "cash": {k: str(v) for k, v in funds.items()},
                "reasons": dict(reasons),
                "history_worker_alive": bool(
                    self.sampler.thread and self.sampler.thread.is_alive()
                ),
                "fees": "0",
                "new_entries_paused": entries_paused,
                "forward_validation": "IN_PROGRESS_NOT_PROFIT_PROVEN",
                "parameters": p.model_dump(mode="json"),
            }
        for order_id in cancels:
            self.queue.cancel(order_id, timestamp)
        # Bound the producer's outstanding commands, not the observed universe.
        if len(queued) < 2 and candidates:
            _, intent = min(candidates, key=lambda x: x[0])
            self.status["last_command"] = self.queue.publish(intent)
        atomic_json(self.root / "STATUS.json", self.status)
        atomic_json(
            self.root / "RANGE_PROFILES.json", {"at": timestamp.isoformat(), "markets": profiles}
        )
