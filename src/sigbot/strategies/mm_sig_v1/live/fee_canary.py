"""Chat-authorized one-share actual BUY/covered SELL fee experiment, never retries POST."""

import fcntl
import json
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from sigbot.accountcheck import read_round
from sigbot.domain import content_hash
from sigbot.errors import AuthorizationRequired, Conflict, SchemaMismatch, SigbotError
from sigbot.orderprobe import ProbeExecutor, exact_body, position_economics
from sigbot.readcheck import ReadBudget, private_write
from sigbot.strategies.mm_sig_v1.live.inputs import review_market
from sigbot.strategies.mm_sig_v1.preflight import REPORTS, RealReads, save

ROOT = Path("runtime/live/strategies/mm_sig_v1/fee-canary")


class FeeCanary:
    def __init__(self):
        self.report: dict = {
            "layer": "LIVE_CANARY",
            "scope": "ONE_SHARE_BUY_AND_COVERED_SELL_FEE_MEASUREMENT",
            "authorization_source": "USER_CHAT_REQUEST_ACTUAL_TRADES_TO_INFER_FEES",
            "max_buy_quantity": 1,
            "max_buy_limit": "0.995",
            "max_submit_attempts": 2,
            "existing_positions_may_be_sold": False,
            "production_approval_granted": False,
            "status": "PREPARING",
            "legs": {},
            "synthetic_data_used": False,
        }
        self.reads = None
        self.probes = []
        self.global_state = Path("runtime/live/order-probes/active.json")

    def persist(self, state):
        self.report["state"] = state
        self.report["updated_at"] = datetime.now(timezone.utc).isoformat()
        self.report["read_requests"] = self.reads.total_attempts if self.reads else 0
        save(ROOT / "active.json", self.report, private=True)
        save(
            self.global_state,
            {"probe_id": "mm-fee-canary", "state": state, "report": self.report},
            private=True,
        )
        save(REPORTS / "FEE_CANARY.json", self.report)

    def leg(self, action, price):
        assert self.reads is not None
        probe = ProbeExecutor(
            self.reads.key,
            "mm-fee-" + action,
            submit_authorized=True,
            cancel_authorized=True,
            root=ROOT / action,
        )
        probe.reader.close()  # All reads use the shared, current, fully scoped session.
        probe.schema = self.reads.reader.schema
        probe.contract = self.reads.reader.contract
        probe.budget = ReadBudget(Path("runtime/readonly/shared-read-budget.json"), 80, 600)
        probe.report.update(
            scope="FEE_CANARY_" + action.upper(),
            authorization_source=self.report["authorization_source"],
            limit_price=str(price),
            fee_model_acceptance="EMPIRICAL_MEASUREMENT_ONLY",
        )
        ROOT.joinpath(action).mkdir(parents=True, exist_ok=True, mode=0o700)
        payload = {
            "exchangeId": self.eid,
            "side": "yes",
            "action": action,
            "quantity": 1,
            "price": price,
            "tournamentId": self.tid,
            "idempotencyKey": "mm-fee-canary-" + action + "-" + self.report["run_id"],
            "expirationDate": (datetime.now(timezone.utc) + timedelta(seconds=90)).isoformat(),
        }
        probe.schema.validate("#/components/schemas/SingleOrderInput", payload)
        probe.body = exact_body(payload)
        private_write(
            probe.private / "intent.json",
            json.dumps(
                {
                    "payload": payload,
                    "scope": self.report["scope"],
                    "nominal_reservation": str(price),
                    "fee_bound": None,
                },
                default=str,
                indent=2,
            ).encode(),
        )
        probe.persist("PREPARED")
        self.probes.append(probe)
        self.report["legs"][action] = probe.report
        self.persist("SUBMITTING")
        placed = probe.write("submit_limit", body=probe.body)
        oid = placed["orderId"]
        if oid is None or placed["exchangeId"] != self.eid:
            raise SchemaMismatch("FEE_CANARY_PLACEMENT_IDENTITY_UNCERTAIN")
        probe.report.update(
            order_id=oid,
            submit_confirmed=True,
            placement={
                k: placed[k]
                for k in (
                    "orderId",
                    "exchangeId",
                    "action",
                    "side",
                    "price",
                    "quantity",
                    "quantityTraded",
                    "fillPrice",
                    "totalCost",
                    "open",
                )
                if k in placed
            },
        )
        self.persist("ACKNOWLEDGED")
        deadline = time.monotonic() + 20
        while True:
            order = self.reads.call("order_detail", path_params={"id": str(oid)})
            fills = self.reads.call(
                "order_fills", path_params={"id": str(oid)}, query={"limit": 200}
            )
            if (
                order["exchangeId"] != self.eid
                or order["tournamentId"] != self.tid
                or order["action"] != action
                or order["side"] != "yes"
            ):
                raise SchemaMismatch("FEE_CANARY_ORDER_SCOPE_CHANGED")
            if fills["pagination"]["hasMore"] or not fills["coverage"]["complete"]:
                raise SchemaMismatch("FEE_CANARY_FILL_COVERAGE_INCOMPLETE")
            quantity = sum((Decimal(str(f["quantity"])) for f in fills["data"]), Decimal(0))
            if quantity not in {Decimal(0), Decimal(1)}:
                raise SchemaMismatch("FEE_CANARY_FILL_QUANTITY_UNEXPECTED")
            if quantity == 1 and not order["open"]:
                cost = sum(
                    (Decimal(str(f["quantity"])) * Decimal(str(f["price"])) for f in fills["data"]),
                    Decimal(0),
                )
                probe.report.update(
                    filled_quantity="1",
                    nominal_notional=str(cost),
                    order_closed=True,
                    actual_fill_rows=fills["data"],
                    cancellation_sent=False,
                )
                self.persist("ACKNOWLEDGED")
                return quantity, cost
            if not order["open"]:
                probe.report.update(filled_quantity=str(quantity), order_closed=True)
                return quantity, Decimal(0)
            if time.monotonic() >= deadline:
                probe.write("cancel_one", order_id=oid)
                order = self.reads.call("order_detail", path_params={"id": str(oid)})
                if order["open"]:
                    raise SchemaMismatch("FEE_CANARY_CANCEL_NOT_CONFIRMED")
                fills = self.reads.call(
                    "order_fills", path_params={"id": str(oid)}, query={"limit": 200}
                )
                quantity = sum((Decimal(str(f["quantity"])) for f in fills["data"]), Decimal(0))
                cost = sum(
                    (Decimal(str(f["quantity"])) * Decimal(str(f["price"])) for f in fills["data"]),
                    Decimal(0),
                )
                probe.report.update(
                    filled_quantity=str(quantity),
                    nominal_notional=str(cost),
                    order_closed=True,
                    cancellation_sent=True,
                    actual_fill_rows=fills["data"],
                )
                return quantity, cost
            time.sleep(2)

    def run(self):
        ROOT.mkdir(parents=True, exist_ok=True, mode=0o700)
        lock_path = Path("runtime/live/order-probes/executor.lock")
        lock_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        with lock_path.open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            if (ROOT / "active.json").exists():
                raise Conflict("FEE_CANARY_ALREADY_ATTEMPTED_NO_AUTOMATIC_RETRY")
            if self.global_state.exists():
                prior = json.loads(self.global_state.read_text())
                if prior.get("state") in {
                    "UNKNOWN",
                    "SUBMITTING",
                    "CANCEL_PENDING",
                    "ACKNOWLEDGED",
                }:
                    raise Conflict("EXISTING_UNRESOLVED_LIVE_PROBE")
                private_write(ROOT / "prior-probe-state.json", self.global_state.read_bytes())
            try:
                self.reads = RealReads(max_requests=80, seconds=600)
                aid, self.tid = self.reads.bootstrap()
                before_result, before = self.reads.checked_account()
                if before_result["status"] != "PASS" or before_result["open_orders"]:
                    raise AuthorizationRequired("FEE_CANARY_ACCOUNT_NOT_RECONCILED_OR_HAS_ORDERS")
                cup = before["cup"]
                if cup["status"] != "active" or cup["isPendingEnrolment"]:
                    raise AuthorizationRequired("FEE_CANARY_TOURNAMENT_NOT_ACTIVE")
                reviews = json.loads((REPORTS / "MARKET_REVIEWS.json").read_text())
                occupied = {
                    p["exchangeId"] for p in before["positions"]["positions"] if p["quantity"]
                }
                candidates = []
                for eid in reviews["selected_exchanges"]:
                    if eid in occupied:
                        continue
                    review = reviews["reviews"][eid]
                    book = self.reads.call(
                        "orderbook",
                        path_params={"id": eid},
                        query={"tournamentId": self.tid, "depth": 20},
                    )
                    if (
                        book["marketId"] != review["market_id"]
                        or book["bestAsk"] is None
                        or book["bestBid"] is None
                    ):
                        continue
                    ask, bid = Decimal(str(book["bestAsk"])), Decimal(str(book["bestBid"]))
                    if (
                        0 < bid < ask <= Decimal("0.995")
                        and ask - bid <= Decimal("0.03")
                        and any(
                            Decimal(str(x["quantity"])) >= 1
                            for x in book["asks"]
                            if Decimal(str(x["price"])) == ask
                        )
                    ):
                        candidates.append((ask, eid, review))
                if not candidates:
                    raise AuthorizationRequired("FEE_CANARY_NO_SMALL_LIQUID_EMPTY_VERIFIED_MARKET")
                _, self.eid, review = min(candidates, key=lambda x: x[0])
                detail = self.reads.call(
                    "market_detail",
                    path_params={"id": review["market_id"]},
                    query={"tournamentId": self.tid},
                )
                actual = review_market(detail, self.reads.market_nodes(detail["id"]), self.tid)
                if (
                    actual["rules_hash"] != review["rules_hash"]
                    or actual["mapping_hash"] != review["mapping_hash"]
                ):
                    raise SchemaMismatch("FEE_CANARY_REVIEW_CHANGED")
                self.report.update(
                    run_id=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S"),
                    account_id_hash=content_hash(aid),
                    tournament_id=self.tid,
                    exchange_id=self.eid,
                    market_id=detail["id"],
                    market_title=detail["title"],
                    before_projected_cash=before_result["projected_cash"],
                    before_reported_cash=before_result["remote_cash"],
                )
                book = self.reads.call(
                    "orderbook",
                    path_params={"id": self.eid},
                    query={"tournamentId": self.tid, "depth": 20},
                )
                if book["marketId"] != detail["id"] or book["bestAsk"] is None:
                    raise SchemaMismatch("FEE_CANARY_CURRENT_BUY_BOOK_INVALID")
                bought, cost = self.leg("buy", Decimal(str(book["bestAsk"])))
                if bought != 1:
                    self.report.update(status="NOT_FILLED", retained_test_position=False)
                    self.persist("CLEAN")
                    return self.report
                between = read_round(self.reads)
                target = [
                    p
                    for p in between["positions"]["positions"]
                    if p["exchangeId"] == self.eid and p["quantity"]
                ]
                if (
                    len(target) != 1
                    or Decimal(str(target[0]["quantity"])) != 1
                    or position_economics(
                        [
                            p
                            for p in between["positions"]["positions"]
                            if p["exchangeId"] != self.eid
                        ]
                    )
                    != position_economics(before["positions"]["positions"])
                ):
                    raise SchemaMismatch("FEE_CANARY_POSITION_OR_EXISTING_HOLDINGS_CHANGED")
                book = self.reads.call(
                    "orderbook",
                    path_params={"id": self.eid},
                    query={"tournamentId": self.tid, "depth": 20},
                )
                if book["marketId"] != detail["id"] or book["bestBid"] is None:
                    raise SchemaMismatch("FEE_CANARY_CURRENT_SELL_BOOK_INVALID")
                bid = Decimal(str(book["bestBid"]))
                if not 0 < bid < 1:
                    raise SchemaMismatch("FEE_CANARY_NO_COVERED_EXIT_BID")
                sold, proceeds = self.leg("sell", bid)
                after_result, after = self.reads.checked_account()
                old_tx = {t["event_id"] for t in before["transactions"]}
                fees = [
                    t
                    for t in after["transactions"]
                    if t["event_id"] not in old_tx and t["event_type"] == "fee"
                ]
                retained = any(
                    p["exchangeId"] == self.eid and p["quantity"]
                    for p in after["positions"]["positions"]
                )
                self.report.update(
                    status="PASS_EMPIRICAL_SCOPE"
                    if sold == 1 and after_result["status"] == "PASS" and not retained
                    else "PARTIAL",
                    buy_nominal_cost=str(cost),
                    sell_nominal_proceeds=str(proceeds),
                    nominal_spread_cost=str(cost - proceeds),
                    after_projected_cash=after_result["projected_cash"],
                    after_reported_cash=after_result["remote_cash"],
                    fee_event_count=len(fees),
                    fee_rows=fees,
                    retained_test_position=retained,
                    account_reconciliation=after_result,
                    before_after_display_cash_difference=str(
                        Decimal(str(before["cup"]["myBalance"]))
                        - Decimal(str(after["cup"]["myBalance"]))
                    ),
                    existing_positions_preserved=position_economics(
                        [p for p in after["positions"]["positions"] if p["quantity"]]
                    )
                    == position_economics(before["positions"]["positions"]),
                    empirical_fee_conclusion="NO_EXTRA_FEE_OBSERVED"
                    if not fees and after_result["status"] == "PASS"
                    else "REVIEW_REQUIRED",
                    general_future_zero_fee_proven=False,
                    precision_limit="CASH_ROUNDED_TO_2_DECIMALS; FEE_EVENTS_AND_NOMINAL_FILLS_CHECKED",
                )
                save(
                    ROOT / "complete-account-evidence.json",
                    {
                        "before": before,
                        "between": between,
                        "after": after,
                        "after_result": after_result,
                    },
                    private=True,
                )
                self.persist(
                    "CLEAN"
                    if not retained and not after_result["open_orders"]
                    else "POSITION_RETAINED"
                )
                return self.report
            except Exception as exc:
                # Failed diagnostic boundary: retain UNKNOWN and stop; never retry a POST.
                self.report.update(
                    status="UNKNOWN" if self.probes else "BLOCKED",
                    code=exc.code
                    if isinstance(exc, SigbotError)
                    else "FEE_CANARY_FATAL_" + type(exc).__name__,
                )
                self.persist("UNKNOWN" if self.probes else "FAILED_UNSENT")
                return self.report
            finally:
                self.report["submit_attempts"] = sum(
                    p.report["submit_attempts"] for p in self.probes
                )
                self.report["cancel_attempts"] = sum(
                    p.report["cancel_attempts"] for p in self.probes
                )
                save(REPORTS / "FEE_CANARY.json", self.report)
                for probe in self.probes:
                    probe.writer.close()
                    probe.key = ""
                if self.reads:
                    self.reads.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Explicit one-share real fee experiment; never auto-retries submissions"
    )
    parser.add_argument("--authorize-one-share-fee-test", action="store_true", required=True)
    parser.parse_args()
    print(json.dumps(FeeCanary().run(), default=str, ensure_ascii=False, indent=2))
