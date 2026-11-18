"""Assertions on actual official receipts and the real PostgreSQL projection only."""

import ast
import hashlib
import json
from decimal import Decimal
from pathlib import Path

from sqlalchemy import select

from sigbot.accountcheck import reconstruct
from sigbot.domain import content_hash
from sigbot.storage import Account, Position, PositionLot
from sigbot.strategies.mm_sig_v1.live.database import open_store
from sigbot.strategies.mm_sig_v1.live.inputs import review_market
from sigbot.strategies.mm_sig_v1.preflight import REPORTS, ROOT, save


def main():
    source_report = json.loads((REPORTS / "ACCOUNT_INITIAL.json").read_text())
    source = Path(source_report["private_evidence"]) / "account-checkpoint.json"
    packet = json.loads(source.read_text(), parse_float=Decimal)
    fee_source = Path("runtime/live/strategies/mm_sig_v1/fee-canary/complete-account-evidence.json")
    if fee_source.exists():
        after = json.loads(fee_source.read_text(), parse_float=Decimal)
        packet = {"data": after["after"], "result": after["after_result"]}
        source = fee_source
    result = reconstruct(packet["data"], cash_display_places=2)
    assert result["status"] == "PASS", result["differences"]
    assert result["positions"] == packet["result"]["positions"]
    checked = 0
    for receipt in source_report["requests"]:
        p = Path(receipt["private_evidence_path"])
        assert hashlib.sha256(p.read_bytes()).hexdigest() == receipt["response_sha256"]
        assert (
            receipt["method"] == "GET"
            and receipt["scope_validated"]
            and receipt["schema_validated"]
        )
        checked += 1
    catalog = json.loads((ROOT / "catalog.json").read_text(), parse_float=Decimal)
    markets = {m["id"]: m for m in catalog}
    reviews = json.loads((REPORTS / "MARKET_REVIEWS.json").read_text())
    tid = packet["data"]["cup"]["id"]
    events = set()
    for eid in reviews["selected_exchanges"]:
        review = reviews["reviews"][eid]
        actual = review_market(markets[review["market_id"]], review["nodes"], tid)
        assert (actual["rules_hash"], actual["mapping_hash"]) == (
            review["rules_hash"],
            review["mapping_hash"],
        )
        assert actual["event_key"] not in events
        events.add(actual["event_key"])
    store = open_store("live")
    try:
        with store.transaction() as s:
            accounts = list(s.scalars(select(Account)))
            assert len(accounts) == 1 and accounts[0].namespace == "live"
            assert not accounts[0].id.startswith("SIM-")
            assert accounts[0].cash == Decimal(result["projected_cash"])
            positions = list(s.scalars(select(Position)))
            lots = list(s.scalars(select(PositionLot)))
            assert len(positions) == len(result["positions"])
            for p in positions:
                owned = [lot for lot in lots if lot.instrument_id == p.instrument_id]
                assert sum((lot.quantity for lot in owned), Decimal(0)) == p.quantity
                assert sum((lot.cost for lot in owned), Decimal(0)) == p.cost
    finally:
        store.engine.dispose()
    state = json.loads((REPORTS / "CURRENT_STATUS.json").read_text())
    assert state["data_mode"] == "REAL_ONLY" and not state["synthetic_data_used"]
    assert state["account_status"] == "PASS" and state["samples"] > 0
    for quote in state["quotes"].values():
        assert Decimal(quote["quote_spread"]) >= Decimal("0.025")
        assert Decimal(quote["bid"]) % Decimal("0.005") == 0
        assert Decimal(quote["ask"]) % Decimal("0.005") == 0
        assert quote["actual_inventory_source"] == "COMPLETE_CURRENT_REAL_ACCOUNT_POSITION_READ"
    for path in Path("src/sigbot/strategies/mm_sig_v1/live").glob("*.py"):
        tree = ast.parse(path.read_text())
        assert not any(
            isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == "FakeExchange"
            for n in ast.walk(tree)
        )
    report = {
        "status": "PASS",
        "layers": ["STATIC", "REAL_READ", "DB_INTEGRATION_REAL_ACCOUNT"],
        "actual_receipts_checked": checked,
        "source_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
        "official_events_reviewed": len(events),
        "actual_db_positions": len(positions),
        "actual_fifo_lots": len(lots),
        "synthetic_data_used": False,
        "mock_tests_run": False,
        "verification_orders_sent": 0,
        "fee_canary_real_orders": 2 if fee_source.exists() else 0,
        "runtime_samples_at_verification": state["samples"],
        "code_identity": content_hash(
            [
                (str(p), hashlib.sha256(p.read_bytes()).hexdigest())
                for p in sorted(Path("src/sigbot/strategies/mm_sig_v1/live").glob("*.py"))
            ]
        ),
    }
    save(REPORTS / "REAL_RECEIPT_VALIDATION.json", report)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
