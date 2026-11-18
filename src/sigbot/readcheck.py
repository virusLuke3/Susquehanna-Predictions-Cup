"""Bounded, operator-invoked REAL_READ check. No ledger import or trading path."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import httpx

from sigbot.config import Config
from sigbot.contracts import Contract
from sigbot.domain import Mode
from sigbot.errors import (
    AuthorizationRequired,
    HistoryReadError,
    RateLimited,
    SchemaMismatch,
    SigbotError,
)
from sigbot.official import DocumentedReads
from sigbot.storage_policy import read_storage_label, save_read_evidence
from sigbot.transport import validate_auth


def load_key(path: str, field: str = "api_key") -> str:
    """Load a single-line opaque value. No shell evaluation, interpolation, or logging."""
    values = []
    for line in Path(path).read_text().splitlines():
        match = re.match(r"^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)$", line)
        if not match or match[1] != field:
            continue
        value = match[2].strip()
        if value.startswith(("'", '"')):
            end = value.find(value[0], 1)
            if end < 0 or (
                value[end + 1 :].strip() and not value[end + 1 :].strip().startswith("#")
            ):
                raise AuthorizationRequired("INVALID_CREDENTIAL_FILE")
            value = value[1:end]
        else:
            value = value.split(" #", 1)[0].rstrip()
        values.append(value)
    if len(values) != 1 or not values[0] or any(c.isspace() for c in values[0]):
        raise AuthorizationRequired("CREDENTIAL_MISSING_OR_AMBIGUOUS")
    validate_auth("Authorization", "Bearer " + values[0])
    return values[0]


def private_write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.parent.chmod(0o700)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as handle:
        handle.write(data)


class ReadBudget:
    """One workspace-wide GET budget across keys; never invent a trading account ledger.

    Count every attempted request before transmission, including failed requests. Persist
    rate-limit backoff across invocations. Other programs outside this workspace remain
    outside this limiter, so an exchange 429 stops the session immediately.
    """

    def __init__(self, path: Path, max_requests: int, seconds: int):
        if not 1 <= max_requests <= 80 or not 1 <= seconds <= 600:
            raise ValueError("invalid bounded read limits")
        self.path = path
        self.max_requests = max_requests
        self.deadline = time.monotonic() + seconds
        self.count = 0

    def charge(self, category: str, weight: int, *, wait=False):
        if category != "READ" or weight != 1:
            raise AuthorizationRequired("READ_ONLY_GET_REQUIRED")
        self._charge("attempts", 100, wait=wait)

    def charge_probe_write(self):
        """Only the bounded probe executor uses this; read transport still rejects writes."""
        self._charge("write_attempts", 30)

    def reserve_gets(self, count, minute_limit=80, *, wait=False):
        """Atomically reserve GETs in the shared rolling window, without artificial pacing."""
        if not 1 <= minute_limit <= 100 or not 1 <= count <= min(3, minute_limit):
            raise ValueError("invalid shared read reservation")
        return self._charge("attempts", minute_limit, count=count, wait=wait)

    def reserve_history_get(self, minute_limit=60, *, wait=False):
        """One atomic debit: history's own cap AND the common 100-GET window."""
        if not 1 <= minute_limit <= 100:
            raise ValueError("invalid history minute limit")
        return self._charge("attempts", 100, wait=wait,
                            secondary=("history_attempts", minute_limit))

    def _charge(self, bucket: str, limit: int, *, count=1, wait=False, secondary=None):
        while True:
            if self.count + count > self.max_requests or time.monotonic() >= self.deadline:
                raise RateLimited("READ_SESSION_LIMIT_REACHED")
            self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
            with os.fdopen(fd, "r+", encoding="utf-8") as handle:
                os.fchmod(handle.fileno(), 0o600)
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
                state = json.loads(handle.read() or '{"attempts":[],"blocked_until":0}')
                now = time.time()
                windows = {}
                available = max(state["blocked_until"], now)
                for name, cap in [(bucket, limit), *([secondary] if secondary else [])]:
                    attempts = [t for t in state.get(name, []) if t > now - 60]
                    windows[name] = attempts
                    excess = len(attempts) + count - cap
                    if excess > 0:
                        available = max(available, sorted(attempts)[excess - 1] + 60)
                if available <= now:
                    times = [now] * count
                    for name, attempts in windows.items():
                        state[name] = [*attempts, *times]
                    handle.seek(0)
                    json.dump(state, handle)
                    handle.truncate()
                    handle.flush()
                    os.fsync(handle.fileno())
                    self.count += count
                    return times
                if not wait:
                    raise RateLimited("SHARED_READ_BUDGET_EXHAUSTED")
                delay = available - now
                if time.monotonic() + delay >= self.deadline:
                    raise RateLimited("READ_SESSION_LIMIT_REACHED")
            # Continuous readers keep their caches alive. Never sleep with the
            # cross-process lock held, and recheck the shared window on wakeup.
            time.sleep(min(1, delay))

    def backoff(self, seconds: int):
        with self.path.open("r+", encoding="utf-8") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            state = json.load(handle)
            state["blocked_until"] = max(state["blocked_until"], time.time() + seconds)
            handle.seek(0)
            json.dump(state, handle)
            handle.truncate()
            handle.flush()
            os.fsync(handle.fileno())


class ReadCheck:
    def __init__(
        self,
        key: str,
        *,
        tournament_slug: str,
        root: Path,
        max_requests: int = 40,
        seconds: int = 180,
        max_pages: int = 10,
        transport: httpx.BaseTransport | None = None,
        contract_file: str = "contracts/api-contract.working.json",
    ):
        if not 1 <= max_pages <= 20:
            raise ValueError("invalid page limit")
        self.key, self.slug, self.max_pages = key, tournament_slug, max_pages
        self.root = root
        self.run_id = uuid.uuid4().hex
        self.private = root / "sessions" / self.run_id
        self.budget = ReadBudget(root / "shared-read-budget.json", max_requests, seconds)
        cfg = Config(
            mode=Mode.READ_ONLY,
            namespace="readonly",
            raw_output_dir="runtime/readonly/raw",
            business_network_enabled=True,
            read_session_authorized=True,
        )
        self.reader = DocumentedReads(
            cfg,
            Contract(contract_file),
            self.budget.charge,
            lambda _: None,
            transport=transport,
            observe=self.observe,
            documented_read_session=True,
        )
        self.requests: list[dict] = []
        self.checks: dict = {}
        self.report: dict = {
            "layer": "MOCK" if isinstance(transport, httpx.MockTransport) else "REAL_READ",
            "validation_scope": "BASIC_READ_ONLY",
            "identity_scope": "API_KEY_HOLDER; OPERATOR_ELIGIBILITY_NOT_VERIFIED",
            "run_id": self.run_id,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "tournament_slug": tournament_slug,
            "read_requests": 0,
            "real_writes": 0,
            "real_orders": 0,
            "real_cancels": 0,
            "account_modifications": 0,
            "production_contract_verified": False,
            "max_requests": max_requests,
            "max_pages_per_endpoint": max_pages,
            "checks": self.checks,
            "requests": self.requests,
            "checkpoint_acceptance": "NOT_PERFORMED",
            "live_acceptance": "NOT_AUTHORIZED_NOT_RUN",
        }

    def observe(self, name: str, status: int, body: bytes):
        path = self.private / f"{len(self.requests) + 1:03d}-{name}.json"
        path = save_read_evidence(path, name, status, body)
        self.requests.append(
            {
                "capability": name,
                "method": "GET",
                "http_status": status,
                "response_sha256": hashlib.sha256(body).hexdigest(),
                "private_evidence_path": str(path) if path else None,
                "storage_policy": read_storage_label(name, status),
                "received_at": datetime.now(timezone.utc).isoformat(),
            }
        )

    def call(self, name: str, **kwargs):
        self.reader.transport.last_response = {}
        data = self.reader.call(name, auth_value="Bearer " + self.key, **kwargs)
        self.requests[-1]["schema_validated"] = True
        if name == "market_list":
            self.requests[-1]["market_page"] = {"rows": len(data["data"]), "pagination": data["pagination"]}
        self.validate_scope(name, data, kwargs)
        self.requests[-1]["scope_validated"] = True
        return data

    def validate_scope(self, name: str, data: dict, args: dict):
        tournament_id = self.reader.config.expected_tournament_id
        path_params = args.get("path_params", {})
        if name == "tournament_identity" and (
            data["id"] != tournament_id or data["slug"] != self.slug
        ):
            raise SchemaMismatch("TOURNAMENT_IDENTITY_MISMATCH")
        if name in {"market_list", "market_detail"}:
            markets = data["data"] if name == "market_list" else [data]
            if name == "market_detail" and data["id"] != path_params["id"]:
                raise SchemaMismatch("MARKET_IDENTITY_MISMATCH")
            for market in markets:
                if not market["contexts"] or any(
                    c["type"] != "tournament"
                    or not c["tournament"]
                    or c["tournament"]["id"] != tournament_id
                    for c in market["contexts"]
                ):
                    raise SchemaMismatch("MARKET_CONTEXT_SCOPE_MISMATCH")
        if name in {"open_orders", "transactions"} and any(
            r["tournamentId"] != tournament_id for r in data["data"]
        ):
            raise SchemaMismatch("ACCOUNT_HISTORY_SCOPE_MISMATCH")
        if name == "bulk_prices":
            requested = set(args["query"]["ids"].split(","))
            returned = [r["exchangeId"] for r in data["data"]]
            missing = data["missingIds"]
            if (
                len(returned) != len(set(returned))
                or len(missing) != len(set(missing))
                or (set(returned) & set(missing) or set(returned) | set(missing) != requested)
            ):
                raise SchemaMismatch("BULK_RESPONSE_ID_MISMATCH")
        if name == "orderbook" and data["exchangeId"] != path_params["id"]:
            raise SchemaMismatch("BOOK_EXCHANGE_ID_MISMATCH")

    def pages(self, name: str, *, allow_archived_orders=False, **kwargs) -> tuple[list, list]:
        self.checks.setdefault(name, {})
        rows: list = []
        coverage: list = []
        query = dict(kwargs.pop("query", {}))
        seen_rows: set[str] = set()
        seen_cursors: set[str] = set()
        total = None
        offset = 0
        restarted = False
        for page in range(self.max_pages):
            try:
                data = self.call(name, query=query, **kwargs)
            except HistoryReadError as exc:
                if exc.code != "CURSOR_EXPIRED" or not query.get("cursor") or restarted:
                    raise
                query.pop("cursor")
                rows, coverage, seen_rows, seen_cursors = [], [], set(), set()
                total, offset, restarted = None, 0, True
                self.checks[name].update(rows_observed=0, coverage=[], cursor_restarted=True)
                continue
            pg = data["pagination"]
            for row in data["data"]:
                if "id" in row or "event_id" in row:
                    identity = str(row.get("id", row.get("event_id")))
                    if identity in seen_rows:
                        raise SchemaMismatch("DUPLICATE_HISTORY_ID")
                    seen_rows.add(identity)
            rows.extend(data["data"])
            if total is not None and pg.get("total", total) != total:
                raise SchemaMismatch("PAGINATION_TOTAL_CHANGED")
            total = pg.get("total", total)
            self.checks[name].update(rows_observed=len(rows), pages_observed=page + 1)
            if "coverage" in data:
                coverage.append(data["coverage"])
                self.checks[name]["coverage"] = coverage
                if data["coverage"]["complete"] is False and not (
                    allow_archived_orders and self.reader.archived_orders_allowed(name, query)
                ):
                    raise SchemaMismatch("REMOTE_COVERAGE_PARTIAL")
            # Retain rows already observed when a later page fails or hits a bound.
            self.checks[name].update(
                rows_observed=len(rows), pages_observed=page + 1, coverage=coverage
            )
            if not pg["hasMore"]:
                if pg.get("nextCursor") is not None or (total is not None and total != len(rows)):
                    raise SchemaMismatch("PAGINATION_TERMINAL_CONFLICT")
                self.checks[name]["pagination_complete"] = True
                return rows, coverage
            if not data["data"]:
                raise SchemaMismatch("CURSOR_LOOP_OR_EMPTY_PAGE")
            if name == "tournament_list":
                if pg["offset"] != offset:
                    raise SchemaMismatch("TOURNAMENT_OFFSET_MISMATCH")
                offset += len(data["data"])
                query["offset"] = offset
            else:
                cursor = pg["nextCursor"]
                if not cursor or cursor in seen_cursors:
                    raise SchemaMismatch("CURSOR_LOOP_OR_EMPTY_PAGE")
                seen_cursors.add(cursor)
                query["cursor"] = cursor
        raise RateLimited("READ_PAGE_LIMIT_REACHED")

    def check(self, name: str, action):
        self.checks[name] = {"status": "RUNNING"}
        try:
            data = action()
        except SigbotError as exc:
            self.checks[name].update(
                status="PARTIAL"
                if self.checks[name].get("pages_observed")
                else "FAIL"
                if isinstance(exc, SchemaMismatch)
                else "BLOCKED",
                code=exc.code,
            )
            self.checks[name].update(self.reader.transport.last_response)
            if (
                isinstance(exc, RateLimited)
                and self.reader.transport.last_response.get("http_status") == 429
            ):
                retry = self.reader.transport.last_response.get("retry_after_seconds", 60)
                self.budget.backoff(retry)
            raise
        self.checks[name]["status"] = "PASS"
        return data

    def independent(self, name: str, action):
        try:
            return self.check(name, action)
        except (AuthorizationRequired, RateLimited):
            raise  # Authentication and shared throttling stop the entire session.
        except SigbotError:
            return None

    def run(self) -> dict:
        try:
            self._run()
        except SigbotError as exc:
            self.report["stop_code"] = exc.code
        finally:
            self.reader.close()
            self.key = ""
        self.report["read_requests"] = self.budget.count
        self.report["finished_at"] = datetime.now(timezone.utc).isoformat()
        required = {
            "account_identity",
            "tournament_list",
            "tournament_identity",
            "market_list",
            "market_detail",
            "bulk_prices",
            "orderbook",
            "open_orders",
            "positions",
            "fills",
            "transactions",
            "settlement_history",
        }
        self.report["not_run"] = sorted(required - self.checks.keys())
        self.report["limits"] = [
            "Market catalog is a paginated observation, not an atomic frozen snapshot or rule approval.",
            "Prices cover at most 100 exchanges; orderbook covers one exchange at depth 20.",
            "Empty orders/fills/positions/settlements do not validate their nonempty economic semantics.",
            "Independent reads may have different watermarks; no real ledger checkpoint or reconciliation acceptance.",
            "API key access does not verify operator identity or tournament eligibility.",
            "No trading, cancellation, account modification, fee or idempotency-retention verification.",
        ]
        self.report["status"] = (
            "PASS"
            if not self.report["not_run"]
            and all(c["status"] == "PASS" for c in self.checks.values())
            and "stop_code" not in self.report
            else "PARTIAL"
            if any(c["status"] == "PASS" for c in self.checks.values())
            else "BLOCKED"
        )
        private_write(
            self.private / "summary.json", json.dumps(self.report, default=str, indent=2).encode()
        )
        return self.report

    def _run(self):
        account = self.check("account_identity", lambda: self.call("account_identity"))
        if not account["id"] or account["id"].startswith("SIM-"):
            raise AuthorizationRequired("INVALID_REAL_ACCOUNT_ID")
        self.reader.config.expected_account_id = account["id"]
        self.report["account_id_sha256"] = hashlib.sha256(account["id"].encode()).hexdigest()
        self.checks["account_identity"]["default_context_balance_available"] = (
            account["balance"] is not None
        )
        tournaments, _ = self.check(
            "tournament_list",
            lambda: self.pages(
                "tournament_list",
                query={"status": "any", "limit": 100, "offset": 0},
            ),
        )
        matches = [t for t in tournaments if t["slug"] == self.slug]
        if len(matches) != 1:
            raise AuthorizationRequired("INTENDED_TOURNAMENT_NOT_UNIQUELY_ACCESSIBLE")
        selected = matches[0]
        self.reader.config.expected_tournament_id = selected["id"]
        self.reader.config.expected_tournament_slug = self.slug
        scoped = {"tournamentId": selected["id"]}
        path = {"slug": self.slug}
        cup = self.check(
            "tournament_identity", lambda: self.call("tournament_identity", path_params=path)
        )
        if cup["id"] != selected["id"] or cup["slug"] != self.slug:
            raise AuthorizationRequired("TOURNAMENT_IDENTITY_MISMATCH")
        self.report["tournament_status"] = cup["status"]
        self.report["pending_enrolment"] = cup["isPendingEnrolment"]
        self.checks["tournament_identity"]["scoped_balance_available"] = (
            cup["myBalance"] is not None
        )
        market_result = self.independent(
            "market_list",
            lambda: self.pages(
                "market_list",
                query={**scoped, "status": "any", "limit": 100},
            ),
        )
        if market_result:
            markets, _ = market_result
            instruments = [e for m in markets for e in m["exchanges"]]
            self.report["markets_observed"] = len(markets)
            self.report["exchanges_observed"] = len(instruments)
            if markets:
                self.independent(
                    "market_detail",
                    lambda: self.call(
                        "market_detail",
                        path_params={"id": markets[0]["id"]},
                        query=scoped,
                    ),
                )
            if instruments:
                sample = list(dict.fromkeys(e["id"] for e in instruments))[:100]
                prices = self.independent(
                    "bulk_prices",
                    lambda: self.call(
                        "bulk_prices",
                        query={**scoped, "ids": ",".join(sample)},
                    ),
                )
                if prices is not None:
                    self.checks["bulk_prices"].update(
                        requested=len(sample),
                        returned=len(prices["data"]),
                        missing=len(prices["missingIds"]),
                        scope="FIRST_100_EXCHANGES_MAXIMUM",
                    )
                    if prices["missingIds"]:
                        self.checks["bulk_prices"]["status"] = "PARTIAL"
                book = self.independent(
                    "orderbook",
                    lambda: self.call(
                        "orderbook",
                        path_params={"id": sample[0]},
                        query={**scoped, "depth": 20},
                    ),
                )
                if book is not None:
                    if book["exchangeId"] != sample[0]:
                        raise SchemaMismatch("BOOK_EXCHANGE_ID_MISMATCH")
                    self.checks["orderbook"].update(
                        bid_levels=len(book["bids"]),
                        ask_levels=len(book["asks"]),
                        version_available=book["asOf"] is not None,
                        scope="ONE_EXCHANGE_DEPTH_20",
                    )
        orders = self.independent(
            "open_orders",
            lambda: self.pages(
                "open_orders",
                query={**scoped, "status": "all", "limit": 200},
            ),
        )
        if orders:
            self.checks["open_orders"]["open_count"] = sum(bool(o["open"]) for o in orders[0])
        positions = self.independent("positions", lambda: self.call("positions", path_params=path))
        if positions is not None:
            self.checks["positions"]["positions_observed"] = len(positions["positions"])
        for name in ["fills", "transactions", "settlement_history"]:
            self.independent(
                name, lambda name=name: self.pages(name, path_params=path, query={"limit": 200})
            )


def run_read_check(args, contract_file: str) -> tuple[dict, int]:
    if not args.authorize_read:
        raise AuthorizationRequired("EXPLICIT_READ_SESSION_AUTHORIZATION_REQUIRED")
    output = Path(args.output)
    if output.suffix != ".json" or output.resolve().parent != Path("reports").resolve():
        raise ValueError("public summary output must not replace private inputs")
    checker = ReadCheck(
        load_key(args.env_file, args.key_name),
        tournament_slug=args.tournament_slug,
        root=Path("runtime/readonly"),
        max_requests=args.max_requests,
        seconds=args.seconds,
        max_pages=args.max_pages,
        contract_file=contract_file,
    )
    report = checker.run()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, default=str, indent=2) + "\n")
    return report, 0 if report["status"] == "PASS" else 2
