"""Resume the known fee BUY by query only, then at most one covered SELL."""

import fcntl
import json
from decimal import Decimal

from sigbot.accountcheck import read_round
from sigbot.domain import content_hash
from sigbot.errors import SchemaMismatch, SigbotError
from sigbot.orderprobe import position_economics
from sigbot.strategies.mm_sig_v1.live.fee_canary import ROOT, FeeCanary
from sigbot.strategies.mm_sig_v1.preflight import REPORTS, RealReads, save


def recover():
    cycle = FeeCanary()
    cycle.report = json.loads((ROOT / "active.json").read_text())
    if cycle.report.get("state") == "CLEAN":
        return cycle.report
    from pathlib import Path

    with Path("runtime/live/order-probes/executor.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            cycle.reads = RealReads(max_requests=80, seconds=600)
            aid, cycle.tid = cycle.reads.bootstrap()
            if (
                content_hash(aid) != cycle.report["account_id_hash"]
                or cycle.tid != cycle.report["tournament_id"]
            ):
                raise SchemaMismatch("FEE_RECOVERY_ACCOUNT_CHANGED")
            cycle.eid = cycle.report["exchange_id"]
            before = json.loads((ROOT / "before-source.json").read_text(), parse_float=Decimal)[
                "data"
            ]["data"]
            oid = cycle.report["legs"]["buy"]["order_id"]
            buy = cycle.reads.call("order_detail", path_params={"id": str(oid)})
            fill_report = cycle.reads.call(
                "order_fills", path_params={"id": str(oid)}, query={"limit": 200}
            )
            rows = fill_report["data"]
            if (
                buy["open"]
                or buy["action"] != "buy"
                or buy["side"] != "yes"
                or buy["exchangeId"] != cycle.eid
                or fill_report["pagination"]["hasMore"]
                or not fill_report["coverage"]["complete"]
            ):
                raise SchemaMismatch("FEE_RECOVERY_BUY_NOT_CONFIRMED")
            quantity = sum((Decimal(str(f["quantity"])) for f in rows), Decimal(0))
            if quantity != 1:
                raise SchemaMismatch("FEE_RECOVERY_BUY_QUANTITY_NOT_ONE")
            cost = sum(
                (Decimal(str(f["quantity"])) * Decimal(str(f["price"])) for f in rows), Decimal(0)
            )
            cycle.report["legs"]["buy"].update(
                filled_quantity="1",
                order_closed=True,
                nominal_notional=str(cost),
                actual_fill_rows=rows,
                recovered_by_read_only=True,
            )
            between = read_round(cycle.reads)
            target = [
                p
                for p in between["positions"]["positions"]
                if p["exchangeId"] == cycle.eid and p["quantity"]
            ]
            if (
                len(target) != 1
                or Decimal(str(target[0]["quantity"])) != 1
                or position_economics(
                    [p for p in between["positions"]["positions"] if p["exchangeId"] != cycle.eid]
                )
                != position_economics(before["positions"]["positions"])
            ):
                raise SchemaMismatch("FEE_RECOVERY_TEST_POSITION_OR_OLD_HOLDINGS_CHANGED")
            if "sell" in cycle.report["legs"] and cycle.report["legs"]["sell"].get(
                "submit_attempts"
            ):
                raise SchemaMismatch("FEE_RECOVERY_SELL_ALREADY_ATTEMPTED_QUERY_ONLY_REQUIRED")
            book = cycle.reads.call(
                "orderbook",
                path_params={"id": cycle.eid},
                query={"tournamentId": cycle.tid, "depth": 20},
            )
            if book["marketId"] != cycle.report["market_id"] or book["bestBid"] is None:
                raise SchemaMismatch("FEE_RECOVERY_NO_CURRENT_EXIT_BOOK")
            sold, proceeds = cycle.leg("sell", Decimal(str(book["bestBid"])))
            result, after = cycle.reads.checked_account()
            old_tx = {t["event_id"] for t in before["transactions"]}
            fees = [
                t
                for t in after["transactions"]
                if t["event_id"] not in old_tx and t["event_type"] == "fee"
            ]
            retained = any(
                p["exchangeId"] == cycle.eid and p["quantity"]
                for p in after["positions"]["positions"]
            )
            cycle.report.update(
                status="PASS_EMPIRICAL_SCOPE"
                if sold == 1 and result["status"] == "PASS" and not retained
                else "PARTIAL",
                buy_nominal_cost=str(cost),
                sell_nominal_proceeds=str(proceeds),
                nominal_spread_cost=str(cost - proceeds),
                after_projected_cash=result["projected_cash"],
                after_reported_cash=result["remote_cash"],
                fee_event_count=len(fees),
                fee_rows=fees,
                retained_test_position=retained,
                account_reconciliation=result,
                existing_positions_preserved=position_economics(
                    [p for p in after["positions"]["positions"] if p["quantity"]]
                )
                == position_economics(before["positions"]["positions"]),
                empirical_fee_conclusion="NO_EXTRA_FEE_OBSERVED"
                if not fees and result["status"] == "PASS"
                else "REVIEW_REQUIRED",
                general_future_zero_fee_proven=False,
                precision_limit="OFFICIAL_CASH_DISPLAY_2DP; FULL_FEE_EVENTS_AND_FILLS_CHECKED",
                maker_fee_measured=False,
                immediate_execution_scope="MARKETABLE_LIMIT_BUY_AND_COVERED_SELL",
            )
            save(
                ROOT / "complete-account-evidence.json",
                {"before": before, "between": between, "after": after, "after_result": result},
                private=True,
            )
            cycle.persist(
                "CLEAN" if not retained and not result["open_orders"] else "POSITION_RETAINED"
            )
        except Exception as exc:
            cycle.report.update(
                status="UNKNOWN",
                code=exc.code
                if isinstance(exc, SigbotError)
                else "FEE_RECOVERY_FATAL_" + type(exc).__name__,
            )
            cycle.persist("UNKNOWN")
        finally:
            cycle.report["submit_attempts"] = sum(
                leg["submit_attempts"] for leg in cycle.report["legs"].values()
            )
            cycle.report["cancel_attempts"] = sum(
                leg["cancel_attempts"] for leg in cycle.report["legs"].values()
            )
            save(ROOT / "active.json", cycle.report, private=True)
            save(REPORTS / "FEE_CANARY.json", cycle.report)
            for probe in cycle.probes:
                probe.writer.close()
                probe.key = ""
            if cycle.reads:
                cycle.reads.close()
        return cycle.report


if __name__ == "__main__":
    print(json.dumps(recover(), default=str, ensure_ascii=False, indent=2))
