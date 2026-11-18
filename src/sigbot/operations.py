from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import TextIO

from sqlalchemy import select, text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import SQLAlchemyError

from sigbot.accounting import Ledger
from sigbot.adapters import AccountSnapshot
from sigbot.domain import ZERO, Clock, utc_time
from sigbot.errors import Conflict, RiskRejected, StorageUnavailable
from sigbot.storage import Account, Order, Outbox, Position, PositionLot, Store


class Controls:
    def __init__(self, store: Store, clock: Clock):
        self.store, self.clock = store, clock

    def initialize(self, snapshot: AccountSnapshot) -> None:
        if not snapshot.coherent or not snapshot.account_id.startswith("SIM-"):
            raise RiskRejected("CHECKPOINT_NOT_VERIFIED")
        self._initialize(snapshot)

    def initialize_live(self, snapshot: AccountSnapshot) -> None:
        """Opening balances include existing/manual holdings after full read acceptance."""
        if (
            self.store.namespace != "live"
            or self.store.engine.dialect.name != "postgresql"
            or snapshot.account_id.startswith("SIM-")
            or not snapshot.coherent
        ):
            raise RiskRejected("LIVE_CHECKPOINT_NOT_VERIFIED")
        self._initialize(snapshot)

    def _initialize(self, snapshot: AccountSnapshot) -> None:
        with self.store.transaction() as s:
            old = s.get(Account, snapshot.account_id)
            if old:
                if (old.namespace, old.tournament) != (
                    self.store.namespace,
                    snapshot.tournament_id,
                ):
                    raise RiskRejected("IDENTITY_MISMATCH")
                return
            if snapshot.orders:
                raise RiskRejected("INITIAL_CHECKPOINT_HAS_UNTRACKED_ORDERS")
            account = Account(
                id=snapshot.account_id,
                namespace=self.store.namespace,
                tournament=snapshot.tournament_id,
                cash=snapshot.cash,
                checkpoint=snapshot.checkpoint,
                lifecycle="RECOVERING",
                reason="CHECKPOINT_IMPORTED",
            )
            s.add(account)
            s.flush()
            ledger = Ledger(self.store)
            payload = {
                "cash": str(snapshot.cash),
                "positions": {k: [str(v) for v in p] for k, p in snapshot.positions.items()},
                "checkpoint": snapshot.checkpoint,
            }
            ledger.event(s, account, "opening:" + snapshot.checkpoint, "OPENING", payload)
            ledger.movement(
                s, account, "opening:" + snapshot.checkpoint, "CASH", snapshot.cash, "OPENING"
            )
            for instrument, (qty, cost) in snapshot.positions.items():
                actual = snapshot.position_lots.get(instrument)
                if actual is not None:
                    from decimal import Decimal

                    if (
                        sum((Decimal(str(entry["quantity"])) for entry in actual), ZERO) != qty
                        or sum(
                            (
                                Decimal(str(entry["quantity"])) * Decimal(str(entry["entryPrice"]))
                                for entry in actual
                            ),
                            ZERO,
                        )
                        != cost
                    ):
                        raise RiskRejected("OPENING_FIFO_LOTS_MISMATCH")
                    for lot in sorted(
                        actual, key=lambda entry: (entry["openedAt"], entry["lotId"])
                    ):
                        s.add(
                            PositionLot(
                                account_id=account.id,
                                instrument_id=instrument,
                                economic_id="opening:" + snapshot.checkpoint + ":" + lot["lotId"],
                                quantity=Decimal(str(lot["quantity"])),
                                cost=Decimal(str(lot["quantity"]))
                                * Decimal(str(lot["entryPrice"])),
                            )
                        )
                else:
                    s.add(
                        PositionLot(
                            account_id=account.id,
                            instrument_id=instrument,
                            economic_id="opening:" + snapshot.checkpoint,
                            quantity=qty,
                            cost=cost,
                        )
                    )
                s.add(
                    Position(
                        account_id=account.id, instrument_id=instrument, quantity=qty, cost=cost
                    )
                )
                ledger.movement(
                    s, account, "opening:" + snapshot.checkpoint, instrument, qty, "OPENING"
                )

    def pause(self, account_id: str, reason: str) -> None:
        with self.store.transaction() as s:
            account = self.store.lock_account(s, account_id)
            account.lifecycle, account.reason = "PAUSED", reason
            self.store.audit(s, account.id, "PAUSE", {"reason": reason}, self.clock.now)

    def recover(self, account_id: str, restored_backup: bool = False) -> None:
        with self.store.transaction() as s:
            account = self.store.lock_account(s, account_id)
            account.lifecycle, account.reason = "RECOVERING", "MANUAL_RESUME_REQUIRED"
            account.reconciled_at = None
            for order in s.scalars(select(Order).where(Order.account_id == account_id)):
                if order.request_state == "SENDING" or (
                    restored_backup and order.request_state == "SEND_PENDING"
                ):
                    order.request_state = "UNKNOWN"
                    outbox = s.get(Outbox, order.id)
                    if outbox:
                        outbox.state = "UNKNOWN"
                if order.cancel_state == "CANCEL_REQUESTED":
                    order.cancel_state = "CANCEL_UNKNOWN"
            self.store.audit(
                s, account_id, "RECOVERY", {"restored_backup": restored_backup}, self.clock.now
            )

    def resume(self, account_id: str, operator: str, *, isolate_unknowns=False) -> None:
        if not operator.strip():
            raise RiskRejected("OPERATOR_REQUIRED")
        with self.store.transaction() as s:
            account = self.store.lock_account(s, account_id)
            if (
                account.reconciled_at is None
                or (self.clock.now - utc_time(account.reconciled_at)).total_seconds() > 45
            ):
                raise RiskRejected("RECONCILIATION_REQUIRED")
            from sigbot.uncertainty import scopes

            pending = scopes(s, account)
            if any(not isolate_unknowns or not item["isolatable"] for item in pending):
                raise RiskRejected("UNRESOLVED_REQUEST")
            account.lifecycle, account.reason = "HEALTHY", "OPERATOR_RESUMED"
            self.store.audit(
                s,
                account_id,
                "RESUME",
                {"operator": operator, "isolated_unknown_orders": pending},
                self.clock.now,
            )

    def status(self, account_id: str) -> dict:
        with self.store.transaction() as s:
            a = self.store.lock_account(s, account_id)
            orders = list(s.scalars(select(Order).where(Order.account_id == account_id)))
            positions = list(s.scalars(select(Position).where(Position.account_id == account_id)))
            from sigbot.uncertainty import scopes

            pending = scopes(s, a, orders)
            return {
                "account": a.id,
                "namespace": a.namespace,
                "lifecycle": a.lifecycle,
                "reason": a.reason,
                "cash": str(a.cash),
                "reservation_total": str(sum((o.reserve_cash for o in orders), ZERO)),
                "unknown_count": sum(o.request_state == "UNKNOWN" for o in orders),
                "in_flight_count": sum(o.request_state == "SENDING" for o in orders),
                "unresolved_requests": pending,
                "isolatable_unknown_count": sum(item["isolatable"] for item in pending),
                "cancel_pending_count": sum(
                    o.cancel_state in {"CANCEL_UNKNOWN", "CANCEL_REQUESTED"} for o in orders
                ),
                "active_order_count": sum(
                    bool(o.reserve_cash or o.reserve_quantity) for o in orders
                ),
                "positions": {
                    p.instrument_id: {
                        "quantity": str(p.quantity),
                        "cost": str(p.cost) if p.cost is not None else None,
                    }
                    for p in positions
                },
                "reconciled_at": a.reconciled_at.isoformat() if a.reconciled_at else None,
            }

    def drain(self, account_id: str, executor) -> dict:
        self.pause(account_id, "DRAINING")
        with self.store.transaction() as s:
            ids = list(s.scalars(select(Order.id).where(Order.account_id == account_id)))
        from sigbot.errors import SigbotError

        errors = []
        for oid in ids:
            try:
                executor.cancel(oid)
            except SigbotError as exc:
                errors.append({"order_id": oid, "code": exc.code})
        status = self.status(account_id)
        status["orders_drained"] = (
            status["active_order_count"] == 0
            and status["unknown_count"] == 0
            and status["cancel_pending_count"] == 0
            and not errors
        )
        status["errors"] = errors
        status["drain_status"] = "DRAINED" if status["orders_drained"] else "DRAIN_INCOMPLETE"
        return status


class Leader:
    """Dedicated PG session advisory lock. SQLite uses flock for mock CLI only."""

    def __init__(self, store: Store, account_id: str):
        self.store, self.account_id = store, account_id
        self.key = int.from_bytes(
            hashlib.sha256(account_id.encode()).digest()[:8], "big", signed=True
        )
        self.conn: Connection | None = None
        self.file: TextIO | None = None
        self.pid: int | None = None
        self.shared = False

    def acquire(self, *, shared=False) -> None:
        if self.conn is not None or self.file is not None:
            raise Conflict("EXECUTOR_ALREADY_STARTED")
        if self.store.engine.dialect.name == "postgresql":
            self.conn = self.store.engine.connect()
            self.shared = shared
            function = "pg_try_advisory_lock_shared" if shared else "pg_try_advisory_lock"
            if not self.conn.scalar(text("SELECT " + function + "(:key)"), {"key": self.key}):
                self.conn.close()
                self.conn = None
                raise Conflict("EXECUTOR_ALREADY_OWNED")
            self.pid = self.conn.scalar(text("SELECT pg_backend_pid()"))
            self.conn.commit()
        else:
            import fcntl

            path = Path("runtime/offline/locks")
            path.mkdir(parents=True, exist_ok=True)
            self.file = (path / str(self.key)).open("a+")
            try:
                fcntl.flock(self.file, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                self.file.close()
                self.file = None
                raise Conflict("EXECUTOR_ALREADY_OWNED") from None

    def check(self) -> None:
        if self.store.engine.dialect.name != "postgresql":
            if self.file is None or self.file.closed:
                raise StorageUnavailable("EXECUTOR_NOT_OWNER")
            return
        try:
            if self.conn is None or self.conn.closed or self.conn.invalidated:
                raise StorageUnavailable("EXECUTOR_LOCK_LOST")
            if self.conn.scalar(text("SELECT pg_backend_pid()")) != self.pid:
                raise StorageUnavailable("EXECUTOR_LOCK_LOST")
            owned = self.conn.scalar(
                text("""SELECT EXISTS (
                SELECT 1 FROM pg_locks WHERE locktype='advisory' AND pid=pg_backend_pid()
                AND classid=:upper AND objid=:lower AND objsubid=1 AND granted
            )"""),
                {"upper": (self.key >> 32) & 0xFFFFFFFF, "lower": self.key & 0xFFFFFFFF},
            )
            if not owned:
                raise StorageUnavailable("EXECUTOR_LOCK_LOST")
            self.conn.commit()
        except SQLAlchemyError:
            raise StorageUnavailable("EXECUTOR_LOCK_LOST") from None

    def close(self) -> None:
        if self.conn is not None:
            try:
                if not self.conn.invalidated:
                    function = "pg_advisory_unlock_shared" if self.shared else "pg_advisory_unlock"
                    self.conn.execute(text("SELECT " + function + "(:key)"), {"key": self.key})
                    self.conn.commit()
            except SQLAlchemyError:
                pass  # Only cleanup; this cannot enable transmission or discard request risk.
            self.conn.close()
            self.conn = None
        if self.file:
            self.file.close()
            self.file = None


def watchdog(heartbeat_path: str, alert_path: str, clock: Clock, max_age: int = 45) -> dict:
    """Separately invocable local observer; never sends email/webhooks."""
    try:
        heartbeat = json.loads(Path(heartbeat_path).read_text())
        age = (clock.now - utc_time(heartbeat["at"])).total_seconds()
        reasons = [] if 0 <= age <= max_age else ["HEARTBEAT_STALE"]
        if heartbeat.get("unknown_count", 0):
            reasons.append("UNKNOWN_ORDERS")
        if heartbeat.get("lifecycle") != "HEALTHY":
            reasons.append("NOT_READY")
    except (OSError, ValueError, KeyError):
        reasons = ["HEARTBEAT_UNAVAILABLE"]
    result = {"at": clock.now.isoformat(), "action_required": bool(reasons), "reasons": reasons}
    Path(alert_path).parent.mkdir(parents=True, exist_ok=True)
    Path(alert_path).write_text(json.dumps(result))
    return result
