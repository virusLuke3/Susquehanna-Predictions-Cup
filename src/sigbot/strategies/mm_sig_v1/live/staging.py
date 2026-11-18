import json
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import select

from sigbot.adapters import AccountSnapshot
from sigbot.domain import ZERO, utc_time
from sigbot.errors import SchemaMismatch
from sigbot.operations import Controls
from sigbot.storage import Account, Instrument, Market, Order, Position, PositionLot


def stage_reviews(store, aid, tid, reviews, markets, result, data, *, update_trading_metadata=True):
    now = datetime.now(timezone.utc)
    market_map = {m["id"]: m for m in markets}
    with store.transaction() as s:
        if not update_trading_metadata and s.get(Account, aid) is not None:
            return {
                "status": "READ_ONLY_CHECKPOINT_ONLY",
                "layer": "REAL_READ",
                "trading_metadata_modified": False,
                "synthetic_data_used": False,
            }
        for review in reviews.values():
            mid, eid = review["market_id"], review["exchange_id"]
            local_market = "SIG-" + tid + "-" + mid
            market = s.get(Market, local_market)
            if market is None:
                market = Market(
                    id=local_market,
                    tournament=tid,
                    api_id=mid,
                    title=review["title"],
                    category="ELECTION",
                    source="OFFICIAL",
                    raw={},
                )
                s.add(market)
            market.rules_hash = review["rules_hash"]
            market.raw = json.loads(
                json.dumps(
                    {"official_market": market_map[mid], "reviewed_sig_nodes": review["nodes"]},
                    default=str,
                )
            )
            s.flush()
            for outcome in ("YES", "NO"):
                iid = "SIG-" + tid + "-" + eid + "-" + outcome
                inst = s.get(Instrument, iid, with_for_update=True)
                changed = inst is None or (inst.rules_hash, inst.mapping_hash) != (
                    review["rules_hash"],
                    review["mapping_hash"],
                )
                if inst is None:
                    inst = Instrument(id=iid)
                    s.add(inst)
                inst.exchange_id, inst.market_id, inst.tournament = eid, local_market, tid
                inst.event_group, inst.outcome = review["event_key"], outcome
                inst.rules_hash, inst.mapping_hash = review["rules_hash"], review["mapping_hash"]
                # A repeated review of the same contract must not erase live
                # safety/quote checkpoints. Changed bindings invalidate them.
                inst.mapping_evidence = {
                    **({} if changed else inst.mapping_evidence),
                    "source": "OFFICIAL",
                    "review": review,
                }
                inst.tick = Decimal(review.get("tick", "0.005"))
                inst.step = Decimal(review.get("lot_step", "1"))
                inst.reviewed, inst.review_until = True, utc_time(data["cup"]["endDate"])
                inst.status = "OPEN"
                if changed:
                    inst.fee_per_share = None
                    inst.quality, inst.book, inst.book_at = "UNKNOWN", {}, None
    if result["status"] != "PASS":
        raise SchemaMismatch("MM_REAL_OPENING_NOT_RECONCILED_OR_HAS_EXTERNAL_ORDERS")
    with store.transaction() as s:
        old = s.get(Account, aid)
        tracked = {o.remote_id for o in s.scalars(select(Order).where(Order.account_id == aid))}
        if any(o["open"] and str(o["id"]) not in tracked for o in data["orders"]):
            raise SchemaMismatch("MM_EXTERNAL_OPEN_ORDER_IMPORT_REQUIRED")
        if old is not None:
            local = list(s.scalars(select(Position).where(Position.account_id == aid)))
            expected = {
                "SIG-" + tid + "-" + key.replace(":", "-"): (
                    Decimal(p["quantity"]),
                    Decimal(p["cost"]),
                )
                for key, p in result["positions"].items()
            }
            if (
                old.cash == Decimal(result["projected_cash"])
                and {p.instrument_id: (p.quantity, p.cost) for p in local if p.quantity} == expected
            ):
                return {
                    "status": "PASS",
                    "layer": "DB_INTEGRATION_REAL_ACCOUNT",
                    "cash": str(old.cash),
                    "positions": len(expected),
                    "fifo_lots": len(
                        list(s.scalars(select(PositionLot).where(PositionLot.account_id == aid)))
                    ),
                    "synthetic_data_used": False,
                    "orders_submitted": 0,
                }
            return {
                "status": "EXISTING_LEDGER_RECOVERY_REQUIRED",
                "layer": "DB_INTEGRATION_REAL_ACCOUNT",
                "synthetic_data_used": False,
                "orders_submitted": 0,
            }
    positions: dict[str, tuple[Decimal, Decimal | None]] = {
        "SIG-" + tid + "-" + key.replace(":", "-"): (Decimal(p["quantity"]), Decimal(p["cost"]))
        for key, p in result["positions"].items()
    }
    lots = {}
    for pos in data["positions"]["positions"]:
        if not pos["quantity"]:
            continue
        iid = "SIG-" + tid + "-" + pos["exchangeId"] + ("-YES" if pos["quantity"] > 0 else "-NO")
        lots[iid] = pos["lots"]
    snapshot = AccountSnapshot(
        aid,
        tid,
        Decimal(result["projected_cash"]),
        positions,
        [],
        result["checkpoint"],
        coherent=True,
        position_lots=lots,
    )
    from sigbot.domain import Clock

    Controls(store, Clock(now)).initialize_live(snapshot)
    with store.transaction() as s:
        account = s.get(Account, aid)
        local_positions = list(s.scalars(select(Position).where(Position.account_id == aid)))
        local_lots = list(s.scalars(select(PositionLot).where(PositionLot.account_id == aid)))
        if (
            account.cash != snapshot.cash
            or {p.instrument_id: (p.quantity, p.cost) for p in local_positions} != positions
        ):
            raise SchemaMismatch("MM_PERSISTED_REAL_LEDGER_MISMATCH")
        if len(local_lots) != sum(len(rows) for rows in lots.values()):
            raise SchemaMismatch("MM_PERSISTED_REAL_FIFO_COUNT_MISMATCH")
        for iid in positions:
            rows = [lot for lot in local_lots if lot.instrument_id == iid]
            if (
                sum((lot.quantity for lot in rows), ZERO),
                sum((lot.cost for lot in rows), ZERO),
            ) != positions[iid]:
                raise SchemaMismatch("MM_PERSISTED_REAL_FIFO_COST_MISMATCH")
        account.checkpoint, account.reconciled_at = result["checkpoint"], now
        account.lifecycle, account.reason = "PAUSED", "MM_ORDER_APPROVAL_AND_FEE_EVIDENCE_REQUIRED"
    return {
        "status": "PASS",
        "layer": "DB_INTEGRATION_REAL_ACCOUNT",
        "cash": str(snapshot.cash),
        "positions": len(positions),
        "fifo_lots": len(local_lots),
        "synthetic_data_used": False,
        "orders_submitted": 0,
    }
