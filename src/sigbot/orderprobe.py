"""One operator-authorized virtual-contest order. Separate from production OMS acceptance."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import httpx

from sigbot.config import Config
from sigbot.contracts import Contract
from sigbot.domain import Mode
from sigbot.errors import (
    AuthorizationRequired,
    Conflict,
    SchemaMismatch,
    SigbotError,
    TransportUnknown,
)
from sigbot.official import DocumentedReads, OfficialSchema
from sigbot.readcheck import ReadBudget, private_write
from sigbot.transport import OFFICIAL_ORIGIN

PRICE = Decimal("0.005")
MAX_TRADE_PRICE = Decimal("0.995")


def exact_body(payload: dict) -> bytes:
    base = {k: v for k, v in payload.items() if k != "price"}
    return (
        json.dumps(base, separators=(",", ":"))[:-1]
        + ',"price":'
        + format(payload["price"], "f")
        + "}"
    ).encode()


def position_economics(positions: list[dict]) -> dict:
    """Exclude changing valuation prices when checking pre-existing holdings."""
    return {
        p["exchangeId"]: (
            p["marketId"],
            p["settled"],
            Decimal(p["quantity"]),
            Decimal(p["costBasis"]),
            sorted(
                (lot["lotId"], lot["side"], Decimal(lot["quantity"]), Decimal(lot["entryPrice"]))
                for lot in p["lots"]
            ),
        )
        for p in positions
    }


class ProbeExecutor:
    """Sole live write executor; one share, one POST, explicit retain or cancel mode.

    Persistent JSON is a diagnostic intent/receipt journal, not a fake production ledger.
    An ambiguous request blocks a new probe. It never retries submission automatically.
    """

    def __init__(
        self,
        key: str,
        probe_id: str,
        *,
        submit_authorized: bool,
        cancel_authorized: bool,
        fill_probe: bool = False,
        root: Path = Path("runtime/live/order-probes"),
        budget_path: Path = Path("runtime/readonly/shared-read-budget.json"),
        transport=None,
    ):
        if not submit_authorized or (not fill_probe and not cancel_authorized):
            raise AuthorizationRequired("ONE_ORDER_SUBMIT_AND_CLEANUP_AUTHORIZATION_REQUIRED")
        if fill_probe and cancel_authorized:
            raise AuthorizationRequired("RETAIN_TRADE_MUST_NOT_AUTHORIZE_CANCEL")
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", probe_id):
            raise ValueError("invalid probe ID")
        self.key, self.probe_id, self.root = key, probe_id, root
        self.fill_probe, self.cancel_authorized = fill_probe, cancel_authorized
        self.limit_price = PRICE
        self.private = root / probe_id
        self.state_path = root / "active.json"
        self.budget = ReadBudget(budget_path, 40, 180)
        self.contract = Contract("contracts/api-contract.working.json")
        self.schema = OfficialSchema()
        self.report: dict = {
            "layer": "MOCK" if isinstance(transport, httpx.MockTransport) else "LIVE_CANARY",
            "scope": "ONE_SHARE_FILL_RETAIN"
            if fill_probe
            else "ONE_LIMIT_ORDER_ACCEPT_QUERY_CANCEL",
            "probe_id": probe_id,
            "status": "PREPARING",
            "submit_attempts": 0,
            "cancel_attempts": 0,
            "read_requests": 0,
            "quantity": 1,
            "limit_price": str(PRICE),
            "tournament_slug": "midterm-elections",
            "production_oms_acceptance": "NOT_PERFORMED",
            "authorization_source": "USER_CHAT_ONE_TRADE_NO_CANCEL"
            if fill_probe
            else "USER_CHAT_SUBMIT_AND_OWN_ORDER_CANCEL",
            "cancel_authorized": cancel_authorized,
            "max_nominal_cost": str(MAX_TRADE_PRICE) if fill_probe else str(PRICE),
            "requests": [],
        }
        cfg = Config(
            mode=Mode.READ_ONLY,
            namespace="readonly",
            raw_output_dir="runtime/readonly/raw",
            business_network_enabled=True,
            read_session_authorized=True,
        )
        self.reader = DocumentedReads(
            cfg,
            self.contract,
            self.budget.charge,
            lambda _: None,
            transport=transport,
            observe=lambda n, s, b: self.observe("GET", n, s, b),
            documented_read_session=True,
        )
        self.writer = httpx.Client(
            transport=transport,
            verify=True,
            trust_env=False,
            follow_redirects=False,
            timeout=httpx.Timeout(15, connect=5),
        )

    def observe(self, method: str, name: str, status: int, body: bytes):
        body = body.replace(self.key.encode(), b"[REDACTED]")
        path = self.private / f"{len(self.report['requests']) + 1:03d}-{name}.json"
        private_write(path, body)
        self.report["requests"].append(
            {
                "method": method,
                "capability": name,
                "http_status": status,
                "response_sha256": hashlib.sha256(body).hexdigest(),
                "private_evidence_path": str(path),
                "at": datetime.now(timezone.utc).isoformat(),
            }
        )

    def persist(self, state: str):
        self.report["state"] = state
        tmp = self.root / "active.tmp"
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as f:
            json.dump(
                {"probe_id": self.probe_id, "state": state, "report": self.report}, f, default=str
            )
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.state_path)
        directory = os.open(self.root, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)

    def get(self, name: str, **kwargs):
        for attempt in range(3):
            self.report["read_requests"] += 1
            try:
                return self.reader.call(name, auth_value="Bearer " + self.key, **kwargs)
            except TransportUnknown:
                if attempt == 2 or self.reader.transport.last_response.get("http_status") != 503:
                    raise
                time.sleep(2)
        raise TransportUnknown("PROBE_READ_UNAVAILABLE")

    def page(self, name: str, **kwargs):
        data = self.get(name, **kwargs)
        if (
            data["pagination"]["hasMore"]
            or data["pagination"].get("nextCursor") is not None
            or data.get("coverage", {}).get("complete") is False
        ):
            raise SchemaMismatch("PROBE_HISTORY_INCOMPLETE")
        return data["data"]

    def write(self, name: str, order_id: int | None = None, body: bytes | None = None):
        cap = self.contract.require_documented_probe(name)
        if name == "cancel_one" and (self.fill_probe or not self.cancel_authorized):
            raise AuthorizationRequired("PROBE_CANCEL_NOT_AUTHORIZED")
        if name == "submit_limit" and (
            body is None
            or body != getattr(self, "body", None)
            or self.report.get("state") != "PREPARED"
        ):
            raise AuthorizationRequired("PROBE_PREPARED_ONE_SHARE_INTENT_REQUIRED")
        if name == "submit_limit" and self.fill_probe and time.monotonic() - self.book_read_at > 15:
            raise AuthorizationRequired("PROBE_BOOK_TOO_OLD")
        if name == "cancel_one" and (order_id is None or order_id != self.report.get("order_id")):
            raise AuthorizationRequired("PROBE_OWN_ORDER_ONLY")
        self.budget.charge_probe_write()
        field = "submit_attempts" if name == "submit_limit" else "cancel_attempts"
        if self.report[field]:
            raise Conflict("PROBE_OPERATION_ALREADY_ATTEMPTED")
        self.report[field] += 1
        self.persist("SUBMITTING" if name == "submit_limit" else "CANCEL_PENDING")
        path = cap["path"].replace("{id}", str(order_id))
        try:
            response = self.writer.request(
                cap["http_method"],
                OFFICIAL_ORIGIN + path,
                headers={"Authorization": "Bearer " + self.key, "Content-Type": "application/json"},
                content=body,
            )
        except httpx.HTTPError:
            raise TransportUnknown("PROBE_WRITE_RESULT_UNKNOWN") from None
        self.observe(cap["http_method"], name, response.status_code, response.content)
        try:
            data = json.loads(response.content, parse_float=Decimal)
        except (ValueError, UnicodeError):
            raise TransportUnknown("PROBE_WRITE_RESULT_UNKNOWN") from None
        if response.status_code != 200:
            codes = {
                "TERMS_NOT_ACKNOWLEDGED",
                "RESIDENCE_UPDATE_REQUIRED",
                "INSUFFICIENT_SCOPES",
                "ACCOUNT_BANNED",
                "FORBIDDEN",
                "VALIDATION_ERROR",
                "INVALID_API_KEY",
                "API_KEY_EXPIRED",
                "RATE_LIMITED",
                "REQUEST_IN_FLIGHT",
                "ORDER_STATUS_UNKNOWN",
                "SERVICE_UNAVAILABLE",
            }
            code = data.get("error", {}).get("code") if isinstance(data, dict) else None
            self.report["remote_error_code"] = code if code in codes else "REMOTE_REQUEST_REJECTED"
            self.report["http_status"] = response.status_code
            if (
                response.status_code >= 500
                or response.status_code == 409
                or response.is_redirect
                or response.status_code < 400
            ):
                raise TransportUnknown("PROBE_WRITE_RESULT_UNKNOWN")
            if response.status_code == 429:
                retry = response.headers.get("Retry-After", "60")
                self.budget.backoff(int(retry) if retry.isdecimal() else 60)
            raise AuthorizationRequired("PROBE_REMOTE_REJECTED")
        self.schema.response(cap["openapi_path"], cap["http_method"].lower(), data)
        return data

    def prepare(self):
        account = self.get("account_identity")
        self.reader.config.expected_account_id = account["id"]
        tournaments = self.page(
            "tournament_list", query={"status": "any", "limit": 100, "offset": 0}
        )
        matches = [t for t in tournaments if t["slug"] == "midterm-elections"]
        if len(matches) != 1:
            raise AuthorizationRequired("PROBE_TOURNAMENT_SCOPE_REQUIRED")
        tid = matches[0]["id"]
        self.reader.config.expected_tournament_id, self.reader.config.expected_tournament_slug = (
            tid,
            "midterm-elections",
        )
        self.path = {"slug": "midterm-elections"}
        self.scoped = {"tournamentId": tid}
        cup = self.get("tournament_identity", path_params=self.path)
        if (
            cup["id"] != tid
            or cup["status"] != "active"
            or cup["isPendingEnrolment"]
            or Decimal(cup["myBalance"]) < PRICE
        ):
            raise AuthorizationRequired("PROBE_TOURNAMENT_NOT_READY")
        self.before_balance = Decimal(cup["myBalance"])
        markets = self.get("market_list", query={**self.scoped, "status": "open", "limit": 100})[
            "data"
        ]
        eligible = [
            m
            for m in markets
            if m["status"] == "open"
            and not m["isComposite"]
            and not m["isMultiOutcome"]
            and len(m["exchanges"]) == 1
        ]
        if not eligible:
            raise SchemaMismatch("PROBE_NO_SIMPLE_OPEN_MARKET")
        market = eligible[0]
        eid = market["exchanges"][0]["id"]
        market = self.get("market_detail", path_params={"id": market["id"]}, query=self.scoped)
        if (
            market["status"] != "open"
            or len(market["exchanges"]) != 1
            or market["exchanges"][0]["id"] != eid
            or not market["contexts"]
            or any(
                c["type"] != "tournament" or not c["tournament"] or c["tournament"]["id"] != tid
                for c in market["contexts"]
            )
        ):
            raise SchemaMismatch("PROBE_MARKET_SCOPE_CHANGED")
        nodes = self.get("market_nodes", path_params={"id": market["id"]}, query=self.scoped)
        positions = self.get("positions", path_params=self.path)
        orders = self.page("open_orders", query={**self.scoped, "status": "all", "limit": 200})
        self.before_positions = positions["positions"]
        if (
            any(o["open"] for o in orders)
            or (not self.fill_probe and self.before_positions)
            or (self.fill_probe and any(p["exchangeId"] == eid for p in self.before_positions))
        ):
            raise AuthorizationRequired("PROBE_EXISTING_EXPOSURE_NEEDS_SPECIFIC_SCOPE")
        self.old_ids = {o["id"] for o in orders}
        if self.fill_probe:
            before_fills = self.page(
                "fills", path_params=self.path, query={"exchangeId": eid, "limit": 200}
            )
            before_transactions = self.page(
                "transactions", path_params=self.path, query={"limit": 200}
            )
            self.old_fill_ids = {f["id"] for f in before_fills}
            self.old_transaction_ids = {t["event_id"] for t in before_transactions}
        book = self.get("orderbook", path_params={"id": eid}, query={**self.scoped, "depth": 20})
        self.book_read_at = time.monotonic()
        if (
            nodes["market_id"] != market["id"]
            or book["exchangeId"] != eid
            or book["marketId"] != market["id"]
            or book["bestAsk"] is None
        ):
            raise SchemaMismatch("PROBE_MARKET_BOOK_MISMATCH")
        ask = Decimal(book["bestAsk"])
        if self.fill_probe:
            if (
                not PRICE <= ask <= MAX_TRADE_PRICE
                or ask % PRICE != 0
                or sum(Decimal(a["quantity"]) for a in book["asks"] if Decimal(a["price"]) == ask)
                < 1
                or self.before_balance < ask
            ):
                raise AuthorizationRequired("PROBE_ONE_SHARE_LIQUIDITY_OR_BALANCE_UNAVAILABLE")
            self.limit_price = ask
            self.report.update(
                limit_price=str(ask), best_ask_at_submit=str(ask), book_version=book["asOf"]
            )
        elif ask <= PRICE:
            raise SchemaMismatch("PROBE_NONMARKETABLE_LIMIT_REQUIRED")
        self.report.update(
            market_id=market["id"],
            exchange_id=eid,
            market_title=market["title"],
            currency_name=cup["currencyName"],
            rules_response_observed=True,
            positions_before=len(self.before_positions),
        )
        payload = {
            "exchangeId": eid,
            "side": "yes",
            "action": "buy",
            "quantity": 1,
            "price": self.limit_price,
            "tournamentId": tid,
            "idempotencyKey": "sigbot-probe-" + self.probe_id,
        }
        if not self.fill_probe:
            payload["expirationDate"] = (
                datetime.now(timezone.utc) + timedelta(minutes=2)
            ).isoformat()
        self.schema.validate("#/components/schemas/SingleOrderInput", payload)
        self.body = exact_body(payload)
        private_write(
            self.private / "intent.json",
            json.dumps(
                {
                    "account_id": account["id"],
                    "payload": payload,
                    "before_balance": self.before_balance,
                    "before_positions": self.before_positions,
                    "nominal_reservation": self.limit_price,
                    "fee_bound": None,
                    "authorization_source": self.report["authorization_source"],
                    "old_fill_ids": sorted(getattr(self, "old_fill_ids", set())),
                    "old_transaction_ids": sorted(getattr(self, "old_transaction_ids", set())),
                },
                default=str,
                indent=2,
            ).encode(),
        )
        self.persist("PREPARED")

    def detail(self):
        data = self.get("order_detail", path_params={"id": str(self.report["order_id"])})
        if (
            data["id"] != self.report["order_id"]
            or data["id"] in self.old_ids
            or data["tournamentId"] != self.scoped["tournamentId"]
            or data["exchangeId"] != self.report["exchange_id"]
            or data["action"] != "buy"
            or data["side"] != "yes"
            or data["priceLimit"] is None
            or Decimal(data["priceLimit"]) != self.limit_price
        ):
            raise SchemaMismatch("PROBE_ORDER_SCOPE_MISMATCH")
        return data

    def execute(self):
        placed = self.write("submit_limit", body=self.body)
        if placed["orderId"] is None or placed["exchangeId"] != self.report["exchange_id"]:
            raise TransportUnknown("PROBE_WRITE_RESULT_UNKNOWN")
        self.report.update(
            order_id=placed["orderId"],
            submit_confirmed=True,
            quantity_traded=str(placed["quantityTraded"]),
            reported_total_cost=str(placed["totalCost"]),
            reported_fill_price=None if placed["fillPrice"] is None else str(placed["fillPrice"]),
        )
        self.persist("ACKNOWLEDGED")
        order = self.detail()
        self.report["query_confirmed"] = True
        if self.fill_probe:
            self.verify_trade()
            return
        if order["open"]:
            canceled = self.write("cancel_one", order_id=placed["orderId"])
            if (
                canceled["orderId"] != placed["orderId"]
                or canceled["tournamentId"] != self.scoped["tournamentId"]
            ):
                raise TransportUnknown("PROBE_CANCEL_RESULT_UNKNOWN")
            self.report["cancel_confirmed"] = True
        else:
            self.report["cancel_confirmed"] = False
        self.verify_final()

    def verify_final(self):
        after_order = self.detail()
        self.report["order_closed_confirmed"] = not after_order["open"]
        self.report["expiration_reported"] = after_order["expirationDate"]
        after_cup = self.get("tournament_identity", path_params=self.path)
        after_positions = self.get("positions", path_params=self.path)
        after_fills = self.page(
            "fills",
            path_params=self.path,
            query={"exchangeId": self.report["exchange_id"], "limit": 200},
        )
        after_orders = self.page(
            "open_orders", query={**self.scoped, "status": "open", "limit": 200}
        )
        self.report.update(
            positions_after=len(after_positions["positions"]),
            open_orders_after=len(after_orders),
            balance_unchanged=Decimal(after_cup["myBalance"]) == self.before_balance,
            fill_rows_after=len(after_fills),
            probe_fill_rows=sum(f["orderId"] == self.report["order_id"] for f in after_fills),
        )
        if after_order["open"] or any(o["id"] == self.report["order_id"] for o in after_orders):
            raise TransportUnknown("PROBE_ORDER_STILL_OPEN")
        self.report["status"] = (
            "PASS"
            if self.report["cancel_confirmed"]
            and self.report["probe_fill_rows"] == 0
            and self.report["balance_unchanged"]
            and not after_positions["positions"]
            else "PARTIAL"
        )
        if self.report["status"] == "PASS" and self.report.get("code"):
            self.report["recovered_read_error"] = self.report.pop("code")
        self.report["finished_at"] = datetime.now(timezone.utc).isoformat()
        self.persist("CLEAN" if self.report["status"] == "PASS" else "CLOSED_REVIEW_REQUIRED")

    def verify_trade(self):
        """Read actual fill, position and cash evidence; leave all exposure untouched."""
        order = self.detail()
        order_fills = self.get(
            "order_fills", path_params={"id": str(self.report["order_id"])}, query={"limit": 200}
        )
        if (
            order_fills["orderId"] != self.report["order_id"]
            or order_fills["exchangeId"] != self.report["exchange_id"]
            or order_fills["tournamentId"] != self.scoped["tournamentId"]
            or order_fills["pagination"]["hasMore"]
            or order_fills["pagination"].get("nextCursor") is not None
            or not order_fills["coverage"]["complete"]
        ):
            raise SchemaMismatch("PROBE_ORDER_FILLS_SCOPE_OR_COVERAGE_MISMATCH")
        cup = self.get("tournament_identity", path_params=self.path)
        positions = self.get("positions", path_params=self.path)["positions"]
        fills = self.page(
            "fills",
            path_params=self.path,
            query={"exchangeId": self.report["exchange_id"], "limit": 200},
        )
        transactions = self.page("transactions", path_params=self.path, query={"limit": 200})
        orders = self.page("open_orders", query={**self.scoped, "status": "open", "limit": 200})
        prices = self.get("bulk_prices", query={**self.scoped, "ids": self.report["exchange_id"]})
        own = [f for f in fills if f["orderId"] == self.report["order_id"]]
        scoped_positions = [p for p in positions if p["exchangeId"] == self.report["exchange_id"]]
        new_transactions = [
            t for t in transactions if t["event_id"] not in self.old_transaction_ids
        ]
        trade_events = [
            t
            for t in new_transactions
            if t["event_type"] == "trade"
            and t["exchangeId"] == self.report["exchange_id"]
            and t["tournamentId"] == self.scoped["tournamentId"]
        ]
        fill_qty = sum((Decimal(f["quantity"]) for f in own), Decimal(0))
        priced = all(f["price"] is not None for f in own)
        fill_cost = (
            sum((Decimal(f["price"]) * Decimal(f["quantity"]) for f in own), Decimal(0))
            if priced
            else None
        )
        cash_debit = self.before_balance - Decimal(cup["myBalance"])
        fill_identity_matches = (
            len({f["id"] for f in own}) == len(own)
            and {f["id"] for f in own} == {f["id"] for f in order_fills["data"]}
            and all(
                f["id"] not in self.old_fill_ids
                and f["marketId"] == self.report["market_id"]
                and f["exchangeId"] == self.report["exchange_id"]
                and f["side"] == "yes"
                and f["price"] is not None
                and PRICE <= Decimal(f["price"]) <= self.limit_price
                and Decimal(f["quantity"]) > 0
                for f in own
            )
            and all(
                any(
                    f["id"] == o["id"]
                    and f["price"] == o["price"]
                    and f["quantity"] == o["quantity"]
                    and f["side"] == o["side"]
                    for o in order_fills["data"]
                )
                for f in own
            )
        )
        position_matches = (
            len(scoped_positions) == 1
            and scoped_positions[0]["marketId"] == self.report["market_id"]
            and not scoped_positions[0]["settled"]
            and Decimal(scoped_positions[0]["quantity"]) == fill_qty == 1
            and Decimal(scoped_positions[0]["costBasis"]) == fill_cost
            and sum((Decimal(lot["quantity"]) for lot in scoped_positions[0]["lots"]), Decimal(0))
            == fill_qty
            and all(lot["side"].lower() == "yes" for lot in scoped_positions[0]["lots"])
            and sum(
                (
                    Decimal(lot["quantity"]) * Decimal(lot["entryPrice"])
                    for lot in scoped_positions[0]["lots"]
                ),
                Decimal(0),
            )
            == fill_cost
        )
        transaction_matches = (
            bool(trade_events)
            and all(
                t["price"] is not None and t["marketId"] == self.report["market_id"]
                for t in trade_events
            )
            and sum((Decimal(t["quantity"]) for t in trade_events), Decimal(0)) == fill_qty
            and sum(
                (Decimal(t["quantity"]) * Decimal(t["price"]) for t in trade_events), Decimal(0)
            )
            == fill_cost
        )
        checks = {
            "order_closed": not order["open"]
            and not any(o["id"] == self.report["order_id"] for o in orders),
            "one_share_filled": fill_qty
            == Decimal(self.report["quantity_traded"])
            == Decimal(order_fills["totalQuantityFilled"])
            == 1,
            "fill_identity": bool(own) and fill_identity_matches,
            "fill_cost_matches_ack": fill_cost is not None
            and fill_cost == Decimal(self.report["reported_total_cost"]),
            "position_and_lots": position_matches,
            "existing_positions_preserved": position_economics(
                [p for p in positions if p["exchangeId"] != self.report["exchange_id"]]
            )
            == position_economics(self.before_positions),
            "balance_debit_matches_cost": fill_cost is not None and cash_debit == fill_cost,
            "new_trade_transaction": transaction_matches,
            "market_price_read": not prices["missingIds"]
            and len(prices["data"]) == 1
            and prices["data"][0]["exchangeId"] == self.report["exchange_id"],
        }
        self.report.update(
            checks=checks,
            status="PASS" if all(checks.values()) else "PARTIAL",
            order_closed_confirmed=not order["open"],
            query_confirmed=True,
            cancel_confirmed=False,
            filled_quantity=str(fill_qty),
            fill_cost=None if fill_cost is None else str(fill_cost),
            balance_debit=str(cash_debit),
            position_quantity=str(
                sum((Decimal(p["quantity"]) for p in scoped_positions), Decimal(0))
            ),
            positions_after=len(positions),
            open_orders_after=len(orders),
            probe_fill_rows=len(own),
            new_trade_transaction_rows=len(trade_events),
            new_fee_event_rows=sum(t["event_type"] == "fee" for t in new_transactions),
            order_fill_coverage=order_fills["coverage"],
            latest_price=None
            if not prices["data"] or prices["data"][0]["latestPrice"] is None
            else str(prices["data"][0]["latestPrice"]),
            retained=True,
            fee_model_acceptance="NOT_PERFORMED",
            checkpoint_acceptance="NOT_PERFORMED",
            finished_at=datetime.now(timezone.utc).isoformat(),
        )
        if self.report["status"] == "PASS" and self.report.get("code"):
            self.report["recovered_read_error"] = self.report.pop("code")
        self.persist(
            "POSITION_RETAINED" if self.report["status"] == "PASS" else "RETAINED_REVIEW_REQUIRED"
        )

    def restore_for_read_verification(self, old: dict):
        """Known accepted/cancelled order only: no submit or cancel is replayed."""
        self.report = old["report"]
        intent = json.loads((self.private / "intent.json").read_text())
        account = self.get("account_identity")
        if account["id"] != intent["account_id"]:
            raise AuthorizationRequired("PROBE_ACCOUNT_CHANGED")
        self.reader.config.expected_account_id = account["id"]
        self.reader.config.expected_tournament_id = intent["payload"]["tournamentId"]
        self.reader.config.expected_tournament_slug = "midterm-elections"
        self.path = {"slug": "midterm-elections"}
        self.scoped = {"tournamentId": intent["payload"]["tournamentId"]}
        self.before_balance = Decimal(intent["before_balance"])
        self.before_positions = intent.get("before_positions", [])
        self.limit_price = Decimal(intent["payload"]["price"])
        self.old_fill_ids = set(intent.get("old_fill_ids", []))
        self.old_transaction_ids = set(intent.get("old_transaction_ids", []))
        before = next(q for q in self.report["requests"] if q["capability"] == "open_orders")
        self.old_ids = {
            o["id"] for o in json.loads(Path(before["private_evidence_path"]).read_text())["data"]
        }
        self.report["read_only_recovery"] = True

    def run(self):
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.root.chmod(0o700)
        with (self.root / "executor.lock").open("a") as lock:
            os.fchmod(lock.fileno(), 0o600)
            try:
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                if self.state_path.exists():
                    old = json.loads(self.state_path.read_text())
                    if (
                        old["probe_id"] == self.probe_id
                        and old["state"] in {"UNKNOWN", "RETAINED_REVIEW_REQUIRED"}
                        and old["report"].get("submit_confirmed")
                        and (
                            (not self.fill_probe and old["report"].get("cancel_confirmed"))
                            or (
                                self.fill_probe
                                and old["report"]["scope"] == "ONE_SHARE_FILL_RETAIN"
                            )
                        )
                    ):
                        self.restore_for_read_verification(old)
                        if self.fill_probe:
                            self.verify_trade()
                        else:
                            self.verify_final()
                        return self.report
                    if old["probe_id"] == self.probe_id or old["state"] not in {
                        "CLEAN",
                        "REJECTED",
                        "FAILED_UNSENT",
                    }:
                        raise Conflict("PROBE_ALREADY_USED_OR_UNRESOLVED")
                self.prepare()
                self.execute()
            except SigbotError as exc:
                self.report.update(
                    status="UNKNOWN"
                    if self.report["submit_attempts"]
                    and isinstance(exc, (TransportUnknown, SchemaMismatch))
                    else "BLOCKED",
                    code=exc.code,
                )
                if self.report["submit_attempts"]:
                    self.persist(
                        "UNKNOWN"
                        if self.report["status"] == "UNKNOWN" or self.report.get("submit_confirmed")
                        else "REJECTED"
                    )
            finally:
                self.reader.close()
                self.writer.close()
                self.key = ""
        self.report["finished_at"] = datetime.now(timezone.utc).isoformat()
        return self.report
