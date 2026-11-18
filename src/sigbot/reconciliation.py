from sqlalchemy import select

from sigbot.adapters import AccountSnapshot
from sigbot.domain import ZERO
from sigbot.errors import SchemaMismatch
from sigbot.storage import EconomicEvent, Journal, Order, Position, PositionLot


class Reconciler:
    def __init__(self, executor):
        self.executor = executor
        self.store = executor.store

    def run(self, snapshot: AccountSnapshot) -> dict:
        e = self.executor
        with self.store.transaction() as s:
            a = self.store.lock_account(s, e.account_id)
            if (a.id, a.tournament) != (snapshot.account_id, snapshot.tournament_id):
                a.lifecycle, a.reason = "PAUSED", "IDENTITY_MISMATCH"
                return {"status": "FAIL", "differences": ["IDENTITY_MISMATCH"]}
            if not snapshot.coherent:
                a.lifecycle, a.reason = "PAUSED", "SNAPSHOT_WINDOW_INDETERMINATE"
                return {"status": "INDETERMINATE", "differences": ["SNAPSHOT_WINDOW"]}
            orders = list(s.scalars(select(Order).where(Order.account_id == a.id)))
            known = {o.id: o for o in orders}
        differences = []
        for obs in snapshot.orders:
            if obs.logical_id not in known:
                differences.append("EXTERNAL_ORDER:" + obs.order_id)
                continue
            e.observe(obs.logical_id, obs)
        for tx in snapshot.transactions:
            if tx["kind"] == "FILL_REFERENCE":
                try:
                    e.ledger.link_transaction(e.account_id, tx["canonical_id"], tx["payload"])
                except SchemaMismatch:
                    differences.append("UNLINKED_TRANSACTION")
            else:
                with self.store.transaction() as s:
                    if s.get(EconomicEvent, (e.account_id, tx["id"])) is None:
                        differences.append("UNIMPORTED_TRANSACTION:" + tx["id"])
        with self.store.transaction() as s:
            a = self.store.lock_account(s, e.account_id)
            from sigbot.uncertainty import scopes

            unresolved = scopes(s, a)
            isolated = [
                item
                for item in unresolved
                if e.config.parallel_strategy_execution and item["isolatable"]
            ]
            isolated_ids = {item["order_id"] for item in isolated}
            positions = list(s.scalars(select(Position).where(Position.account_id == a.id)))
            local = {p.instrument_id: (p.quantity, p.cost) for p in positions if p.quantity != ZERO}
            remote = {k: v for k, v in snapshot.positions.items() if v[0] != ZERO}
            if local != remote:
                differences.append("POSITION_OR_COST_MISMATCH")
            if a.cash != snapshot.cash:
                differences.append("CASH_MISMATCH")
            observed = {o.logical_id for o in snapshot.orders}
            for order in s.scalars(select(Order).where(Order.account_id == a.id)):
                if (
                    order.remote_id
                    and order.state not in {"FILLED", "CANCELED", "REJECTED", "EXPIRED", "CLOSED"}
                    and order.id not in observed
                ):
                    differences.append("REMOTE_ORDER_MISSING")
                if (
                    order.request_state in {"SENDING", "UNKNOWN"}
                    or order.cancel_state == "CANCEL_UNKNOWN"
                ) and order.id not in isolated_ids:
                    differences.append("UNRESOLVED_REQUEST")
                if order.observed_filled != order.filled:
                    differences.append("MISSING_FILL_EVIDENCE")
            journal_assets: dict = {}
            owned_assets: dict = {}
            for entry in s.scalars(select(Journal).where(Journal.account_id == a.id)):
                journal_assets[entry.asset] = journal_assets.get(entry.asset, ZERO) + entry.amount
                if entry.bucket == "ACCOUNT":
                    owned_assets[entry.asset] = owned_assets.get(entry.asset, ZERO) + entry.amount
            if any(v != ZERO for v in journal_assets.values()):
                differences.append("JOURNAL_UNBALANCED")
            if owned_assets.get("CASH", ZERO) != a.cash:
                differences.append("CASH_PROJECTION_MISMATCH")
            for pos in positions:
                lots = list(
                    s.scalars(
                        select(PositionLot).where(
                            PositionLot.account_id == a.id,
                            PositionLot.instrument_id == pos.instrument_id,
                        )
                    )
                )
                if (
                    sum((lot.quantity for lot in lots), ZERO) != pos.quantity
                    or owned_assets.get(pos.instrument_id, ZERO) != pos.quantity
                ):
                    differences.append("POSITION_PROJECTION_MISMATCH")
                if pos.cost is None or any(lot.cost is None for lot in lots):
                    differences.append("UNKNOWN_COST_BASIS")
                elif sum((lot.cost for lot in lots if lot.cost is not None), ZERO) != pos.cost:
                    differences.append("LOT_COST_PROJECTION_MISMATCH")
            if differences:
                a.lifecycle, a.reason = "PAUSED", "RECONCILIATION_DIFFERENCE"
                a.reconciled_at = None
            else:
                a.reconciled_at = e.clock.now
            result = {
                "status": "PASS" if not differences else "FAIL",
                "differences": sorted(set(differences)),
                "checkpoint": snapshot.checkpoint,
                "isolated_unknown_orders": isolated,
                "fully_resolved": not unresolved,
                "execution_scope": "PARTIAL" if unresolved else "ALL_RECONCILED",
                "layer": "MOCK"
                if e.exchange.synthetic or getattr(e.exchange, "mock", False)
                else "REAL_READ",
            }
            self.store.audit(s, a.id, "RECONCILIATION", result, e.clock.now)
        return result
