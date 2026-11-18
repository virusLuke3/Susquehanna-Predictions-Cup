"""Persistent ECR observation. Real GET receipts, no order executor or paper fills.

Raw title matches and gross quote gaps are diagnostics, never trade approvals.
Minute sampling coverage is distinct from strategy-admissible source freshness.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import signal
import subprocess
import threading
import time
from collections import Counter, deque
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import ExitStack
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from sqlalchemy import select, text
from sqlalchemy.exc import SQLAlchemyError

from sigbot.domain import utc_time
from sigbot.errors import (
    AuthorizationRequired,
    ContractNotVerified,
    RateLimited,
    SchemaMismatch,
    SigbotError,
    TransportUnknown,
)
from sigbot.readcheck import ReadBudget, ReadCheck, load_key
from sigbot.realtime import direct_process_proxy_environment, documented_realtime
from sigbot.storage import Account, Audit, Store
from sigbot.storage_policy import bounded_storage, read_storage_label, save_read_evidence
from sigbot.strategies.ecr_sig_v1 import core
from sigbot.strategies.ecr_sig_v1.correspondence import correspondences
from sigbot.strategies.ecr_sig_v1.depth import DepthCollector
from sigbot.strategies.ecr_sig_v1.inputs import CandidateInput
from sigbot.strategies.ecr_sig_v1.liveinputs import MinuteSeries, evaluate, fresh
from sigbot.strategies.ecr_sig_v1.proxy import RabisuProxy
from sigbot.strategies.ecr_sig_v1.publicdata import (
    PublicReads,
    discovery_query,
    kalshi_candidates,
    levels,
    matched,
    monitoring_matches,
    polymarket_candidates,
    refresh_known_contracts,
    utcnow,
)
from sigbot.strategies.prc_sig_v1.catalog import catalog_rows, refresh_reference

ROOT = Path("runtime/readonly/strategies/ecr_sig_v1")
REPORT = Path("reports/strategies/ecr_sig_v1/OBSERVER_STATUS.json")
PUBLIC_FAILURES = (SigbotError, KeyError, ValueError, TypeError, subprocess.TimeoutExpired)


def atomic_json(path: Path, value):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temp = path.with_suffix(".tmp")
    raw = json.dumps(value, default=str, ensure_ascii=False, indent=2).encode()
    with temp.open("wb") as f:
        os.fchmod(f.fileno(), 0o600)
        f.write(raw)
        f.flush()
        os.fsync(f.fileno())
    temp.replace(path)


def append_json(path: Path, value):
    from sigbot.storage_policy import MARKET_JOURNALS
    if bounded_storage() and path.name in MARKET_JOURNALS:
        return
    fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(json.dumps(value, default=str, ensure_ascii=False) + "\n")
        f.flush()
        os.fsync(f.fileno())


class ContinuousReads(ReadCheck):
    """Reuse official schema, scope and workspace budget; archive every response."""

    def __init__(self, key, contract_file, realtime_contract_file=None):
        self.realtime_contract_file = realtime_contract_file
        super().__init__(
            key,
            tournament_slug="midterm-elections",
            root=Path("runtime/readonly"),
            max_requests=80,
            seconds=600,
            max_pages=20,
            contract_file=contract_file,
        )
        self.requests = deque(maxlen=100)  # type: ignore[assignment]
        self.sequence = 0
        self.total_attempts = 0
        self.count_by_capability: Counter = Counter()
        self.reader.transport.charge = self.charge
        # The reviewed realtime snapshot also contains current v1 GET schemas
        # (nullable tournament balance and explicit order termination fields).
        reviewed, self.reader.schema = (
            documented_realtime(realtime_contract_file)
            if realtime_contract_file
            else documented_realtime()
        )
        evidence = {
            "source": "OFFICIAL",
            "evidence_path": self.reader.schema.path,
            "sha256": hashlib.sha256(Path(self.reader.schema.path).read_bytes()).hexdigest(),
            "source_url": "https://sig.thesuper.market/api/v1/docs",
            "observed_at": reviewed.get("observed_at", "2026-10-04"),
            "review_policy": "Pinned reviewed schema; compare current reference before ECR reads",
        }
        # Rebind only this read session's evidence to the reviewed snapshot.
        # The original capability register, production enablement and write
        # evidence remain unchanged. Every response still passes its JSON schema.
        for cap in self.reader.contract.data["capabilities"].values():
            if cap.get("http_method") == "GET" and cap.get("status") in {"DOCUMENTED", "VERIFIED"}:
                if cap.get("openapi_path") not in self.reader.schema.spec["paths"]:
                    raise ContractNotVerified("ECR_PINNED_READ_PATH_MISSING")
                cap["evidence"] = [deepcopy(evidence)]

    def charge(self, category, weight):
        self.renew_budget()
        try:
            self.budget.charge(category, weight, wait=True)
        except RateLimited as exc:
            if exc.code != "READ_SESSION_LIMIT_REACHED":
                raise
            # The local window can expire while waiting for the shared budget lock.
            # This failure precedes recording/sending. Retry once with a new local window;
            # the same shared file still enforces global pacing, limits and backoff.
            self.budget = ReadBudget(self.root / "shared-read-budget.json", 80, 600)
            self.budget.charge(category, weight, wait=True)
        self.total_attempts += 1

    def renew_budget(self):
        if self.budget.count >= 80 or time.monotonic() >= self.budget.deadline:
            self.budget = ReadBudget(self.root / "shared-read-budget.json", 80, 600)

    def call(self, name, **kwargs):
        try:
            return super().call(name, **kwargs)
        except RateLimited:
            response = self.reader.transport.last_response
            if response.get("http_status") == 429:
                self.budget.backoff(response.get("retry_after_seconds", 60))
            raise

    def observe(self, name, status, body):
        self.sequence += 1
        self.count_by_capability[name] += 1
        path = self.private / f"{self.sequence:09d}-{name}.json"
        path = save_read_evidence(path, name, status, body)
        self.requests.append(
            {
                "capability": name,
                "method": "GET",
                "http_status": status,
                "response_sha256": hashlib.sha256(body).hexdigest(),
                "private_evidence_path": str(path) if path else None,
                "storage_policy": read_storage_label(name, status),
                "received_at": utcnow().isoformat(),
            }
        )
        if read_storage_label(name, status) == "RETAINED":
            append_json(self.private / "receipts.jsonl", self.requests[-1])


def sig_book(data: dict, row: dict, received: datetime, started: datetime | None = None) -> dict:
    if data["marketId"] != row["api_market_id"] or data["exchangeId"] not in row["exchange_ids"]:
        raise SchemaMismatch("ECR_SIG_BOOK_IDENTITY_MISMATCH")
    bids, asks = levels(data["bids"], "quantity"), levels(data["asks"], "quantity")
    version = data["asOf"]
    source = datetime.fromisoformat(version["at"]) if version else None
    if source and source.tzinfo is None:
        raise SchemaMismatch("ECR_SIG_SOURCE_TIME_NOT_UTC")
    if data.get("source") == "REALTIME" and data.get("received_at"):
        delivered = datetime.fromisoformat(data["received_at"])
        if delivered.tzinfo is None:
            raise SchemaMismatch("ECR_SIG_RECEIPT_TIME_NOT_UTC")
        if delivered > received:
            # Use the actual receipt of a newer full book, not its older REST
            # validation. The collector excludes books with unresolved gaps.
            received = started = delivered
    return {
        "bids": bids,
        "asks": asks,
        "received_at": received.isoformat(),
        "request_started_at": (started or received).isoformat(),
        "source_at": source.isoformat() if source else None,
        "source_time_status": "OFFICIAL_AS_OF" if source else "MISSING_OFFICIAL_AS_OF",
        "version": version,
        "bid": max((Decimal(x["price"]) for x in bids), default=None),
        "ask": min((Decimal(x["price"]) for x in asks), default=None),
        "fresh": bool(
            source
            and source <= received
            and 0 <= (received - (started or received)).total_seconds() <= 5
        ),
    }


def minute_coverage(samples: list[dict], start: int) -> dict:
    times = sorted({float(x["at"]) for x in samples if start <= float(x["at"]) < start + 60})
    boundaries = [float(start), *times, float(start + 60)]
    gaps = [b - a for a, b in zip(boundaries, boundaries[1:], strict=False)]
    sample_ok = len(times) >= 3 and max(gaps, default=60) <= 25
    return {
        "at": datetime.fromtimestamp(start + 60, timezone.utc).isoformat(),
        "samples": len(times),
        "maximum_sample_gap_seconds": max(gaps, default=60),
        "sampling_complete": sample_ok,
        "strategy_complete": sample_ok and all(x["strategy_ready"] for x in samples),
        "no_imputation": True,
        "scope": "REST_SAMPLES_NOT_ALL_TICKS",
    }


def evaluated_inputs(packets: list[CandidateInput], now: datetime) -> dict:
    """Same ECR core for reviewed real input; this creates no account/order state."""
    accepted, rejected = [], {}
    for packet in packets:
        try:
            if any(
                x.startswith("SIM-")
                for x in [packet.key, packet.instrument_id, packet.exchange_id, packet.market_id]
            ):
                raise core.Reject("synthetic input in real observer")
            candidate = packet.candidate(now, synthetic=False)
            result = core.signal(candidate, int(now.timestamp()))
            accepted.append({"key": packet.key, "route": result.route})
        except core.Reject as exc:
            rejected[packet.key] = str(exc)
    return {
        "qualified_signals": accepted,
        "rejected": rejected,
        "real_orders": 0,
        "expected_pnl": None,
        "authorized_for_live": False,
    }


class Observer:
    def __init__(
        self,
        reads,
        public,
        root=ROOT,
        report=REPORT,
        collector=None,
        shared_store=None,
        account_ttl_seconds=45,
    ):
        self.reads, self.public, self.root, self.report_path = reads, public, root, report
        self.collector = collector
        self.shared_store = shared_store
        self.account_ttl_seconds = account_ttl_seconds
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.rows: list[dict] = []
        self.external: list[dict] = []
        self.collection_seeds: list[dict] = []
        self.search_queue: deque = deque()
        self.kalshi_queue: deque = deque()
        self.search_coverage: dict = {}
        self.kalshi_coverage: dict = {}
        self.quote_results: list[dict] = []
        self.price_pairs: list = []
        self.price_baselines: dict = {}
        self.price_previous: dict = {}
        self.price_last_record: dict = {}
        price_map = self.root / "price-monitoring-market-map.json"
        self.price_market_ids = json.loads(price_map.read_text()) if price_map.exists() else {}
        if not isinstance(self.price_market_ids, dict) or any(
            not isinstance(k, str) or not isinstance(v, str)
            for k, v in self.price_market_ids.items()
        ):
            raise SchemaMismatch("ECR_PRICE_MONITORING_MAP_INVALID")
        self.quotes: dict = {}
        self.samples: dict[str, deque] = {}
        self.minutes: dict[str, deque] = {}
        self.last_closed: dict[str, int] = {}
        self.rules_observed: set[str] = set()
        self.nodes: dict[str, dict] = {}
        self.pair_checks: list[dict] = []
        self.series: dict[str, MinuteSeries] = {}
        self.evaluations: list[dict] = []
        self.rules_cursor = 0
        self.focus: list[str] = []
        self.next_catalog = self.next_quotes = self.next_discovery = self.next_rules = 0.0
        self.next_series = 0.0
        self.metadata_pool = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="sig-metadata-read"
        )
        self.metadata_future: Future | None = None
        self.metadata_stop = threading.Event()
        self.failures: Counter = Counter()
        self.state = {
            "strategy_id": "ecr_sig",
            "strategy_version": "1.0",
            "mode": "READ_ONLY",
            "layer": "REAL_READ",
            "run_id": reads.run_id,
            "started_at": utcnow().isoformat(),
            "lifecycle": "STARTING",
            "real_orders": 0,
            "real_cancels": 0,
            "account_modifications": 0,
            "qualified_signals": 0,
            "executable_plans": 0,
            "fee": {
                "status": "UNKNOWN",
                "buy_fee_bound": None,
                "sell_fee_bound": None,
                "reason": "Participation free wording does not define SUSQies trading fees.",
                "official_api_fill_fee_field": "NOT_PROVIDED",
                "source": "https://sig.thesuper.market/docs/markets-and-trading",
            },
            "account_checkpoint": "NOT_ACCEPTED",
            "normal_live_bridge_enabled": False,
            "restart_history_policy": "NEW_PROCESS_REQUIRES_FRESH_CONTIGUOUS_WINDOWS",
            "observation_method": "SIG_REALTIME_EXTERNAL_BATCH_REST_SAMPLING",
            "synthetic_fills": 0,
            "signals_evaluated": 0,
            "signal_engine_status": "BLOCKED_BEFORE_CORE_EVALUATION_UNVERIFIED_INPUTS",
        }

    def bootstrap(self, *, on_catalog_ready=None):
        r = self.reads
        account = r.check("account_identity", lambda: r.call("account_identity"))
        if not account["id"] or account["id"].startswith("SIM-"):
            raise AuthorizationRequired("ECR_REAL_ACCOUNT_REQUIRED")
        r.reader.config.expected_account_id = account["id"]
        self.state["account_id_sha256"] = hashlib.sha256(account["id"].encode()).hexdigest()
        cups, _ = r.check(
            "tournament_list",
            lambda: r.pages("tournament_list", query={"status": "any", "limit": 100, "offset": 0}),
        )
        matches = [c for c in cups if c["slug"] == r.slug]
        if len(matches) != 1:
            raise AuthorizationRequired("ECR_TOURNAMENT_AMBIGUOUS")
        self.tid = matches[0]["id"]
        r.reader.config.expected_tournament_id = self.tid
        r.reader.config.expected_tournament_slug = r.slug
        self.cup = r.check(
            "tournament_identity",
            lambda: r.call("tournament_identity", path_params={"slug": r.slug}),
        )
        if self.cup["isPendingEnrolment"]:
            raise AuthorizationRequired("ECR_ENROLMENT_PENDING")
        self.state["tournament_id"] = self.tid
        self.sync_catalog()
        if on_catalog_ready is not None:
            on_catalog_ready(self)
        seed_path = self.root / "external-candidates.json"
        seeds = json.loads(seed_path.read_text()) if seed_path.exists() else []
        self.collection_seeds = seeds
        self.restore_discovery()
        if seeds:
            try:
                fresh_contracts, refresh_status = refresh_known_contracts(self.public, seeds)
                self.external = fresh_contracts
                self.collection_seeds = []
                self.state["known_external_refresh"] = refresh_status
                atomic_json(seed_path, self.external)
            except PUBLIC_FAILURES as exc:
                self.failures[
                    exc.code
                    if isinstance(exc, SigbotError)
                    else "ECR_KNOWN_EXTERNAL_REFRESH_FAILED"
                ] += 1
        self.state["lifecycle"] = "OBSERVING_TRADE_GATES_BLOCKED"
        self.poll_account()
        self.publish()

    def restore_discovery(self):
        # Discovery metadata survives a restart; price windows do not. Verify
        # the actual anonymous HTTP receipt and bytes, then run the same parser.
        restored = []
        receipts = 0
        rejected: Counter = Counter()
        for path in sorted(
            [
                *self.reads.private.parent.glob("*/external/*-POLYMARKET_SEARCH.json"),
                *self.reads.private.parent.glob("*/external/*-POLYMARKET_MARKETS.json"),
            ],
            key=lambda p: p.stat().st_mtime_ns,
        ):
            receipt_path = path.with_suffix(".receipt.json")
            if not receipt_path.is_file():
                continue
            receipt = json.loads(receipt_path.read_text())
            observed = datetime.fromisoformat(receipt["received_at"])
            if not 0 <= (utcnow() - observed).total_seconds() <= 3600:
                continue
            body = path.read_bytes()
            known_markets = path.name.endswith("-POLYMARKET_MARKETS.json")
            expected_url = (
                "https://gamma-api.polymarket.com/markets?"
                if known_markets else "https://gamma-api.polymarket.com/public-search?"
            )
            if (
                receipt.get("http_status") != 200
                or receipt.get("auth_used") is not False
                or not receipt.get("url", "").startswith(expected_url)
                or hashlib.sha256(body).hexdigest() != receipt.get("response_sha256")
            ):
                continue
            try:
                decoded = json.loads(body)
                if known_markets:
                    if not isinstance(decoded, list):
                        raise SchemaMismatch("ECR_ARCHIVED_GAMMA_MARKETS_NOT_ARRAY")
                    found = []
                    for market in decoded:
                        events = market.get("events") or []
                        if len(events) == 1:
                            found.extend(polymarket_candidates({
                                "events": [{**events[0], "markets": [market]}],
                                "pagination": {},
                            }))
                else:
                    found = polymarket_candidates(decoded)
            except (json.JSONDecodeError, SchemaMismatch, KeyError, TypeError):
                # A received HTTP 200 can still be a failed read. Its original
                # bytes/receipt remain intact and never become valid metadata.
                rejected["ARCHIVED_SEARCH_BODY_INVALID"] += 1
                continue
            restored.extend(found)
            receipts += 1
        if restored:
            self.merge_external(restored)
        self.state["restored_discovery_receipts"] = receipts
        self.state["restored_discovery_rejections"] = dict(rejected)
        self.state["restored_discovery_scope"] = "PROVISIONAL_METADATA_ONLY_NEW_REAL_BOOKS_REQUIRED"

    def poll_account(self):
        """Observe the gateway's persisted proof; never rebuild account history here."""
        result: dict = {"status": "UNAVAILABLE", "source": "SHARED_GATEWAY_DATABASE"}
        if self.shared_store is not None:
            aid = self.reads.reader.config.expected_account_id
            try:
                with self.shared_store.transaction() as session:
                    if self.shared_store.engine.dialect.name == "postgresql":
                        session.execute(
                            text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
                        )
                    account = session.get(Account, aid)
                    if account is not None:
                        if (account.namespace, account.tournament) != (
                            self.shared_store.namespace,
                            self.tid,
                        ):
                            raise AuthorizationRequired("ECR_SHARED_ACCOUNT_IDENTITY_MISMATCH")
                        audit = session.scalar(
                            select(Audit)
                            .where(Audit.account_id == aid, Audit.kind == "RECONCILIATION")
                            .order_by(Audit.id.desc())
                            .limit(1)
                        )
                        if audit is not None:
                            age = (utcnow() - utc_time(audit.at)).total_seconds()
                            result.update(
                                deepcopy(audit.data),
                                source="SHARED_GATEWAY_DATABASE",
                                gateway_status=audit.data.get("status"),
                                observed_at=audit.at.isoformat(),
                                age_seconds=age,
                                account_id=aid,
                                tournament_id=account.tournament,
                                account_lifecycle=account.lifecycle,
                            )
                            if not result.get("checkpoint") or (
                                self.shared_store.namespace == "live"
                                and result.get("layer") != "REAL_READ"
                            ):
                                result["status"] = "UNVERIFIED_SOURCE"
                            elif result.get("status") != "PASS":
                                pass  # Retain the gateway's actual failure and differences.
                            elif audit.at != account.reconciled_at:
                                result["status"] = "NOT_CURRENT"
                            elif not 0 <= age <= self.account_ttl_seconds:
                                result["status"] = "STALE"
                            elif account.lifecycle != "HEALTHY":
                                result["status"] = "NOT_HEALTHY"
            except SQLAlchemyError:
                result = {
                    "status": "UNAVAILABLE",
                    "source": "SHARED_GATEWAY_DATABASE",
                    "error_code": "SHARED_ACCOUNT_DATABASE_READ_FAILED",
                }
        self.state["account_read_reconciliation"] = result
        self.state["account_checkpoint"] = "SHARED_GATEWAY_" + result["status"]
        self.state["account_observation_current"] = result["status"] == "PASS"
        self.state["account_acceptance_owner"] = "SHARED_GATEWAY"
        # Observation does not import or independently approve an executable ledger.
        self.state["production_oms_ledger_acceptance"] = False

    def sync_catalog(self):
        r = self.reads
        before = r.sequence
        markets, _ = r.check(
            "market_list",
            lambda: r.pages(
                "market_list", query={"tournamentId": self.tid, "status": "any", "limit": 100}
            ),
        )
        self.rows = catalog_rows(markets, self.tid)
        self.state["catalog"] = {
            "complete": True,
            "markets": len(self.rows),
            "statuses": dict(Counter(x["status"] for x in self.rows)),
            "observed_at": utcnow().isoformat(),
            "pages": r.sequence - before,
            "consistency": "NON_ATOMIC_PAGINATED_OBSERVATION",
        }
        atomic_json(self.root / "catalog.json", self.rows)
        queries = list(dict.fromkeys(q for row in self.rows if (q := discovery_query(row))))
        # Frequent SIG catalog refresh must not reset progress to the first states.
        pending = [q for q in self.search_queue if q in queries]
        pending.extend(
            q
            for q in queries
            if q not in pending
            and (
                q not in self.search_coverage
                or (
                    utcnow() - datetime.fromisoformat(self.search_coverage[q]["observed_at"])
                ).total_seconds()
                > 3600
            )
        )
        self.search_queue = deque(pending)
        self.next_catalog = time.monotonic() + 600

    def discover(self):
        if self.search_queue:
            query = self.search_queue.popleft()
            page, found = 1, []
            while page <= 3:
                data = self.public.get(
                    "POLYMARKET_SEARCH", {"q": query, "limit_per_type": 50, "page": page}
                )
                found.extend(polymarket_candidates(data))
                pg = data["pagination"]
                if not isinstance(pg.get("hasMore"), bool):
                    raise SchemaMismatch("ECR_SEARCH_PAGINATION_INVALID")
                self.search_coverage[query] = {
                    "pages": page,
                    "complete": not pg["hasMore"],
                    "total_results": pg.get("totalResults"),
                    "observed_at": utcnow().isoformat(),
                }
                if not pg["hasMore"]:
                    break
                page += 1
            self.merge_external(found)
        if not self.kalshi_queue and time.monotonic() >= self.next_series:
            series = self.public.get("KALSHI_SERIES", {"category": "Politics"})["series"]
            if not isinstance(series, list):
                raise SchemaMismatch("ECR_SERIES_SCHEMA_INVALID")
            self.kalshi_queue = deque(
                s["ticker"]
                for s in series
                if re.search(r"\b(senate|governor|house)\b", s["title"], re.I)
                and re.search(r"\b(win|winner|election|party)\b", s["title"], re.I)
            )
            self.state["kalshi_series_discovery"] = {
                "category_filter": "Politics",
                "returned": len(series),
                "relevant": len(self.kalshi_queue),
                "scope": "SERIES_METADATA_ONLY",
            }
            self.next_series = time.monotonic() + 3600
        if self.kalshi_queue:
            ticker = self.kalshi_queue.popleft()
            query = {"series_ticker": ticker, "status": "open", "limit": 100}
            cursors: set = set()
            for page in range(1, 4):
                data = self.public.get("KALSHI_MARKETS", query)
                self.merge_external(kalshi_candidates(data))
                cursor = data["cursor"]
                self.kalshi_coverage[ticker] = {
                    "pages": page,
                    "complete": not cursor,
                    "observed_at": utcnow().isoformat(),
                }
                if not cursor:
                    break
                if cursor in cursors or not data["markets"]:
                    raise SchemaMismatch("ECR_KALSHI_CURSOR_LOOP")
                cursors.add(cursor)
                query["cursor"] = cursor
        atomic_json(
            self.root / "discovery-coverage.json",
            {"polymarket": self.search_coverage, "kalshi": self.kalshi_coverage},
        )
        self.next_discovery = time.monotonic() + 20

    def merge_external(self, found):
        existing = {(x["operator"], x["contract_id"]): x for x in self.external}
        existing.update({(x["operator"], x["contract_id"]): x for x in found})
        self.external = list(existing.values())
        atomic_json(self.root / "external-candidates.json", self.external)

    def prices(self):
        started = time.monotonic()
        if self.collector is not None and getattr(self.collector, "shared_output", None):
            self.quotes = self.collector.bulk_prices()
            exchanges = [
                e for row in self.rows if row["status"] == "open" for e in row["exchange_ids"]
            ]
            self.state["bulk_quotes"] = {
                "complete": set(exchanges).issubset(self.quotes),
                "requested": len(exchanges),
                "returned": len(set(exchanges) & set(self.quotes)),
                "observed_at": utcnow().isoformat(),
                "source_time": "NOT_PROVIDED",
                "method": "SHARED_INDEPENDENT_SIG_PRICE_WORKER",
            }
            append_json(
                self.root / "quotes.jsonl", {"at": utcnow().isoformat(), "quotes": self.quotes}
            )
            self.next_quotes = started + 2
            return
        self.quotes = {}
        self.state["bulk_quotes"] = {"complete": False, "status": "UPDATING"}
        expected = {e: r["api_market_id"] for r in self.rows for e in r["exchange_ids"]}
        exchanges = [e for row in self.rows if row["status"] == "open" for e in row["exchange_ids"]]
        current = {}
        for start in range(0, len(exchanges), 100):
            data = self.reads.call(
                "bulk_prices",
                query={"tournamentId": self.tid, "ids": ",".join(exchanges[start : start + 100])},
            )
            for item in data["data"]:
                if item["marketId"] != expected[item["exchangeId"]]:
                    raise SchemaMismatch("ECR_BULK_MARKET_IDENTITY_MISMATCH")
                current[item["exchangeId"]] = {
                    **item,
                    "received_at": utcnow().isoformat(),
                    "source_at": None,
                }
            if data["missingIds"]:
                self.failures["BULK_MISSING_IDS"] += len(data["missingIds"])
        self.quotes = current
        if self.collector is not None and hasattr(self.collector, "set_bulk_prices"):
            self.collector.set_bulk_prices(current)
        self.state["bulk_quotes"] = {
            "complete": len(current) == len(exchanges),
            "requested": len(exchanges),
            "returned": len(current),
            "observed_at": utcnow().isoformat(),
            "source_time": "NOT_PROVIDED",
        }
        append_json(self.root / "quotes.jsonl", {"at": utcnow().isoformat(), "quotes": current})
        self.next_quotes = (
            started + 15
            if self.collector is not None and getattr(self.collector, "shared_output", None)
            else time.monotonic() + 20
        )

    def observe_rules(self):
        eligible = [r for r in self.rows if r["event_candidate"]["office"] != "CHAMBER_CONTROL"]
        if not eligible:
            return
        # Focused candidates must not wait for a round trip through the whole directory.
        unseen_focus = [
            r
            for r in eligible
            if matched(r, self.external) and r["api_market_id"] not in self.nodes
        ]
        unseen = [r for r in eligible if r["api_market_id"] not in self.nodes]
        row = (
            (unseen_focus or unseen)[0]
            if unseen_focus or unseen
            else eligible[self.rules_cursor % len(eligible)]
        )
        self.rules_cursor += 1
        data = self.reads.call(
            "market_nodes",
            path_params={"id": row["api_market_id"]},
            query={"tournamentId": self.tid},
        )
        self.nodes[row["api_market_id"]] = data
        atomic_json(self.root / "observed-nodes.json", self.nodes)
        self.rules_observed.add(row["api_market_id"])
        self.next_rules = time.monotonic() + (2 if unseen else 20)

    def books(self):
        if self.collector is not None and getattr(self.collector, "shared_output", None):
            self.quotes = self.collector.bulk_prices()
        cached_path = self.root / "audited-nodes.json"
        if cached_path.exists():
            captured = json.loads(cached_path.read_text())
            for mid, item in captured.items():
                observed = datetime.fromisoformat(item["observed_at"])
                if mid not in self.nodes and 0 <= (utcnow() - observed).total_seconds() <= 600:
                    self.nodes[mid] = item["nodes"]
                    self.rules_observed.add(mid)
        pairs = [(r, matched(r, self.external)) for r in self.rows if r["status"] == "open"]
        # Acquire candidate books before admission/rule audits complete. Cached
        # restart IDs may be probed, but never enter signal evaluation as fresh
        # metadata; failed refreshes remain explicit gaps.
        self.price_pairs = [
            (r, choices)
            for r in self.rows
            if r["status"] == "open"
            and (
                choices := monitoring_matches(
                    r, self.external, market_id=self.price_market_ids.get(r["api_market_id"])
                )
            )
        ]
        self.pair_checks = correspondences(self.rows, self.nodes, self.external, self.tid)
        # Compare rules before choosing a token; known margin contracts are excluded.
        checks = {
            (x["api_market_id"], x["operator"], x["external_contract_id"]): x
            for x in self.pair_checks
        }
        pairs = [
            (
                r,
                [
                    c
                    for c in m
                    if checks.get((r["api_market_id"], c["operator"], c["contract_id"]), {}).get(
                        "unique_operator_match"
                    )
                ],
            )
            for r, m in pairs
        ]
        pairs = [(r, m) for r, m in pairs if m]
        # Collection has no three-card cap or hourly rotation. Both parties of
        # the same election are collected; event grouping remains in risk logic.
        self.focus = [r["api_market_id"] for r, _ in pairs]
        local_cache, external_cache = {}, {}
        if self.collector is not None:
            self.collector.update_catalog(self.rows)
            collection_targets = [c for _, choices in self.price_pairs + pairs for c in choices]
            self.collector.set_targets(collection_targets, market_ids=self.focus)
            local_cache, external_cache, status = self.collector.snapshot()
            self.state["depth_collector"] = status
            self.state["depth_collector"]["metadata_seed_status"] = (
                "STALE_IDS_FOR_COLLECTION_ONLY" if self.collection_seeds else "CURRENT_METADATA"
            )
        self.monitor_prices(local_cache, external_cache)
        output = []
        evaluations = []
        for row, contracts in pairs:
            try:
                if self.collector is None:
                    raise SchemaMismatch("ECR_DEPTH_COLLECTOR_NOT_RUNNING")
                raw = local_cache.get(row["exchange_ids"][0])
                if raw is None:
                    raise SchemaMismatch("ECR_SIG_BOOK_PENDING_BASELINE_OR_REPAIR")
                received = datetime.fromisoformat(
                    raw.get("validation_received_at") or raw["received_at"]
                )
                started = datetime.fromisoformat(
                    raw.get("validation_requested_at") or raw["received_at"]
                )
                local = sig_book(raw, row, received, started)
                local["collection_method"] = raw["source"]
                external = []
                for operator in ["POLYMARKET", "KALSHI"]:
                    choices = [x for x in contracts if x["operator"] == operator]
                    # Ambiguous contracts can be observed separately; never collapse
                    # them into one approved operator signal or select by higher bid.
                    for c in sorted(choices, key=lambda c: c["contract_id"])[:2]:
                        book = external_cache.get((operator, c["contract_id"]))
                        if book is None:
                            raise SchemaMismatch("ECR_EXTERNAL_BOOK_MISSING_THIS_ROUND")
                        external.append(book)
                diagnostic = {
                    "api_market_id": row["api_market_id"],
                    "title": row["title"],
                    "sig": local,
                    "external": external,
                    "at": utcnow().timestamp(),
                    "strategy_ready": False,
                    "raw_bid_minus_sig_ask": {
                        x["operator"] + ":" + x["contract_id"]: str(x["bid"] - local["ask"])
                        for x in external
                        if x["bid"] is not None and local["ask"] is not None
                    },
                    "interpretation": "UNAPPROVED_TITLE_MATCH_GROSS_GAP_ONLY_NOT_SIGNAL",
                    "blocking_reasons": [],
                }
                now = utcnow()
                diagnostic["strategy_ready"] = bool(
                    external and all(fresh(b, now) for b in [local, *external])
                )
                diagnostic["interpretation"] = "VERIFIED_EVENT_PREDICATE_GROSS_GAP_NOT_NET_SIGNAL"
                pair = checks[
                    (row["api_market_id"], contracts[0]["operator"], contracts[0]["contract_id"])
                ]
                series = self.series.setdefault(row["api_market_id"], MinuteSeries())
                binding = (
                    pair["sig_rules_hash"],
                    tuple(
                        (
                            c["operator"],
                            c["contract_id"],
                            checks[(row["api_market_id"], c["operator"], c["contract_id"])][
                                "external_rules_hash"
                            ],
                        )
                        for c in contracts
                    ),
                )
                for minute in series.add(diagnostic, binding):
                    persisted = {**minute, "api_market_id": row["api_market_id"]}
                    if minute["quote"]:
                        from dataclasses import asdict

                        persisted["quote"] = asdict(minute["quote"])
                    append_json(self.root / "price-minutes.jsonl", persisted)
                evaluation = evaluate(row, pair, diagnostic, series, self.state["fee"], now)
                self.state["signals_evaluated"] += int(evaluation["core_called"])
                evaluations.append(evaluation)
                diagnostic["blocking_reasons"] = evaluation["reasons"]
                samples = self.samples.setdefault(row["api_market_id"], deque(maxlen=180))
                # Never keep last good quotes as current on failed requests.
                samples.append(diagnostic)
                output.append(diagnostic)
                now_bucket = int(diagnostic["at"] // 60) * 60
                start = now_bucket - 60
                if self.last_closed.get(row["api_market_id"]) != start:
                    bucket = [s for s in samples if start <= s["at"] < start + 60]
                    coverage = {
                        "api_market_id": row["api_market_id"],
                        **minute_coverage(bucket, start),
                    }
                    self.minutes.setdefault(row["api_market_id"], deque(maxlen=60)).append(coverage)
                    append_json(self.root / "minute-coverage.jsonl", coverage)
                    self.last_closed[row["api_market_id"]] = start
            except PUBLIC_FAILURES as exc:
                self.failures[
                    exc.code if isinstance(exc, SigbotError) else "PUBLIC_SCHEMA_OR_TIMEOUT"
                ] += 1
                output.append(
                    {
                        "api_market_id": row["api_market_id"],
                        "status": "DATA_GAP",
                        "reason": exc.code
                        if isinstance(exc, SigbotError)
                        else "PUBLIC_SCHEMA_OR_TIMEOUT",
                        "at": utcnow().isoformat(),
                    }
                )
        self.quote_results = output
        self.evaluations = evaluations
        atomic_json(self.root / "correspondences.json", self.pair_checks)
        atomic_json(self.root / "signal-evaluations.json", evaluations)
        append_json(
            self.root / "book-observations.jsonl",
            {"at": utcnow().isoformat(), "observations": output},
        )

    def monitor_prices(self, local_cache=None, external_cache=None):
        """Observe all unique questions, independently of executable rule reviews."""
        if self.collector is None or not self.price_pairs:
            return
        if local_cache is None or external_cache is None:
            local_cache, external_cache, _ = self.collector.snapshot()
        quotes = self.collector.bulk_prices() if hasattr(self.collector, "bulk_prices") else {}
        at = utcnow()
        # A complete broadcast already supplies an observed price even while
        # REST continuity repair is pending. This copy is price-only: never add
        # these books to the caller's executable snapshot or refresh timestamps.
        local_cache = dict(local_cache)
        pending = {}
        if hasattr(self.collector, "monitoring_snapshot"):
            observed, feed = self.collector.monitoring_snapshot()
            pending = feed.get("pending_reasons", {})
            proof = feed.get("connection_checked_at")
            if (
                feed.get("subscription_status") == "SUBSCRIBED"
                and proof
                and 0 <= (at - utc_time(proof)).total_seconds() <= 5
            ):
                local_cache.update(
                    {
                        eid: book
                        for eid, book in observed.items()
                        if eid not in local_cache
                        and book.get("source") == "REALTIME"
                        and book.get("asOf")
                        and (not book.get("nextExpiryAt") or utc_time(book["nextExpiryAt"]) > at)
                    }
                )
        records, changes = [], []

        def quote(bid, ask, received, origin, ttl, key):
            prices = [Decimal(str(v)) if v is not None else None for v in (bid, ask)]
            if any(p is not None and (not p.is_finite() or not 0 <= p <= 1) for p in prices):
                raise SchemaMismatch("ECR_MONITOR_PRICE_INVALID")
            age = (at - utc_time(received)).total_seconds() if received else None
            bid_price, ask_price = prices
            mid = (
                (bid_price + ask_price) / 2
                if bid_price is not None and ask_price is not None and bid_price <= ask_price
                else None
            )
            status = (
                "MISSING" if mid is None or age is None else "FRESH" if 0 <= age <= ttl else "STALE"
            )
            if age is not None and 0 <= age <= ttl and (bid_price is None) != (ask_price is None):
                status = "ONE_SIDED"
            baseline = self.price_baselines.setdefault(key, mid) if status == "FRESH" else None
            previous = self.price_previous.get(key)
            result = {
                "bid": prices[0],
                "ask": prices[1],
                "mid": mid,
                "received_at": received,
                "age_seconds": age,
                "source": origin,
                "status": status,
                "mid_change_pp": (mid - previous) * 100
                if status == "FRESH" and previous is not None
                else None,
                "mid_change_since_start_pp": (mid - baseline) * 100
                if baseline is not None
                else None,
            }
            if status == "FRESH":
                self.price_previous[key] = mid
            return result

        for row, choices in self.price_pairs:
            mid, eid = row["api_market_id"], row["exchange_ids"][0]
            bulk, raw = quotes.get(eid, {}), local_cache.get(eid)
            for contract in choices:
                key = (mid, contract["operator"], contract["contract_id"])
                if bulk and (bulk.get("marketId") != mid or bulk.get("exchangeId") != eid):
                    raise SchemaMismatch("ECR_MONITOR_SIG_IDENTITY_MISMATCH")
                bid, ask, received = (
                    bulk.get("bestBid"),
                    bulk.get("bestAsk"),
                    bulk.get("received_at"),
                )
                origin = "SIG_BULK_PRICES"
                if raw is not None:
                    book = sig_book(
                        raw, row, utc_time(raw.get("validation_received_at") or raw["received_at"])
                    )
                    if not received or utc_time(book["received_at"]) > utc_time(received):
                        bid, ask, received = book["bid"], book["ask"], book["received_at"]
                        origin = "SIG_VERIFIED_BOOK"
                sig = quote(bid, ask, received, origin, 30, (*key, "SIG"))
                sig["depth_pending_reasons"] = pending.get(mid, [])
                if origin == "SIG_VERIFIED_BOOK" and pending.get(mid):
                    sig["source"] = "SIG_REALTIME_PRICE_OBSERVATION"
                book = external_cache.get((contract["operator"], contract["contract_id"]), {})
                external = quote(
                    book.get("bid"),
                    book.get("ask"),
                    book.get("received_at"),
                    contract["operator"] + "_BOOK",
                    10,
                    (*key, "EXTERNAL"),
                )
                current = sig["status"] == external["status"] == "FRESH"
                record = {
                    "sig_market_id": mid,
                    "title": row["title"],
                    "exchange_id": eid,
                    "operator": contract["operator"],
                    "external_market_id": contract.get("market_id"),
                    "external_contract_id": contract["contract_id"],
                    "sig": sig,
                    "external": external,
                    "status": "CURRENT" if current else "DATA_GAP",
                    "external_bid_minus_sig_ask": external["bid"] - sig["ask"] if current else None,
                    "execution_approved": False,
                }
                fingerprint = tuple((q["bid"], q["ask"], q["status"]) for q in (sig, external))
                if self.price_last_record.get(key) != fingerprint:
                    changes.append(record)
                    self.price_last_record[key] = fingerprint
                records.append(record)
        frame = {
            "observed_at": at.isoformat(),
            "monitored_cards": len(self.price_pairs),
            "current_pairs": sum(r["status"] == "CURRENT" for r in records),
            "gap_pairs": sum(r["status"] == "DATA_GAP" for r in records),
            "one_sided_pairs": sum(
                "ONE_SIDED" in (r["sig"]["status"], r["external"]["status"]) for r in records
            ),
            "sig_max_age_seconds": 30,
            "external_max_age_seconds": 10,
            "records": records,
            "interpretation": "OBSERVED_PRICES_NOT_EXECUTION_ADMISSION",
        }
        self.state["price_monitoring"] = frame
        atomic_json(self.root / "cross-market-prices.json", frame)
        if changes:
            append_json(
                self.root / "cross-market-price-changes.jsonl",
                {"observed_at": at.isoformat(), "changes": changes},
            )

    def cycle(self):
        self.poll_account()
        if self.metadata_future is not None and self.metadata_future.done():
            completed, self.metadata_future = self.metadata_future, None
            completed.result()
        self.state["lifecycle"] = "OBSERVING_TRADE_GATES_BLOCKED"
        self.books()
        self.publish()
        if self.metadata_future is None:
            self.metadata_future = self.metadata_pool.submit(self.metadata_loop)

    def metadata_loop(self):
        while not self.metadata_stop.is_set():
            try:
                self.metadata_cycle()
                self.state["metadata_read_status"] = "RUNNING"
            except (RateLimited, TransportUnknown) as exc:
                # A background metadata budget wait must not stop book sampling.
                # Authentication, schema and contract failures still propagate.
                self.failures[exc.code] += 1
                self.state["metadata_read_status"] = "RETRYING_FAILED_READ"
                self.metadata_stop.wait(60)
            self.metadata_stop.wait(0.25)

    def metadata_cycle(self):
        now = time.monotonic()
        # Quantity books and minute decisions precede broad discovery and bulk
        # metadata. Periodic full-account reads run independently so they cannot
        # create a deterministic hole every five minutes in a 15-minute window.
        if now >= self.next_catalog:
            self.sync_catalog()
        if now >= self.next_quotes:
            self.prices()
        # Publish healthy SIG progress before potentially slow external requests.
        if now >= self.next_discovery:
            try:
                self.discover()
            except PUBLIC_FAILURES as exc:
                self.failures[
                    exc.code if isinstance(exc, SigbotError) else "PUBLIC_SCHEMA_OR_TIMEOUT"
                ] += 1
                self.next_discovery = time.monotonic() + 60
        if now >= self.next_rules:
            self.observe_rules()

    def publish(self):
        potential = [(r, matched(r, self.external)) for r in self.rows]
        matched_cards = sum(bool(xs) for _, xs in potential)
        control = sum(r["event_candidate"]["office"] == "CHAMBER_CONTROL" for r in self.rows)
        predicate_cards = {
            x["api_market_id"] for x in self.pair_checks if x["unique_operator_match"]
        }
        equivalent_cards = {
            x["api_market_id"]
            for x in self.pair_checks
            if x["unique_operator_match"] and x["settlement_equivalent"]
        }
        actual_reasons = Counter(reason for x in self.evaluations for reason in x["reasons"])
        actual_reasons.update(
            x["reason"] for x in self.pair_checks if not x["unique_operator_match"]
        )
        self.state.update(
            updated_at=utcnow().isoformat(),
            pid=os.getpid(),
            authenticated_get_attempts=self.reads.total_attempts,
            authenticated_get_counter_scope="MAIN_SESSION_ONLY_NO_ACCOUNT_HISTORY_READS",
            responses_by_capability=dict(self.reads.count_by_capability),
            public_get_attempts=self.public.count,
            observed_rules_cards=len(self.rules_observed),
            approved_rule_cards=len(equivalent_cards),
            verified_event_predicate_cards=len(predicate_cards),
            correspondence_results=self.pair_checks,
            signal_evaluations=self.evaluations,
            signal_engine_status="RUNNING_ADMISSION_CHECKS"
            if self.state["signals_evaluated"]
            else "COLLECTING_COMPLETE_WINDOWS",
            qualified_signals=sum(x["qualified"] for x in self.evaluations),
            provisional_external_contracts=len(self.external),
            provisional_matched_sig_cards=matched_cards,
            discovery={
                "pending_polymarket_queries": len(self.search_queue),
                "polymarket_queries_observed": len(self.search_coverage),
                "pending_kalshi_series": len(self.kalshi_queue),
                "kalshi_series_observed": len(self.kalshi_coverage),
                "full_external_universe_proven": False,
            },
            focused_book_cards=self.focus,
            book_collection_scope="ALL_UNIQUE_EVENT_PREDICATE_MATCHED_CARDS",
            latest_book_observations=self.quote_results,
            minute_sampling_complete=sum(
                x["sampling_complete"] for m in self.minutes.values() for x in m
            ),
            strategy_complete_minutes=sum(
                x["strategy_complete"] for s in self.series.values() for x in s.minutes
            ),
            failures=dict(self.failures),
            blocking_reasons=dict(actual_reasons),
            writes_capability="ABSENT_FROM_OBSERVER",
            expected_pnl=None,
        )
        self.state["coverage_funnel"] = {
            "SIG_catalog_markets": len(self.rows),
            "control_observe_only": control,
            "provisional_external_matched_cards": matched_cards,
            "focused_full_book_cards": len(self.focus),
            "full_book_targets": len(self.focus),
            "both_platform_books_observed": sum(
                "sig" in x and bool(x.get("external")) for x in self.quote_results
            ),
            "approved_equivalence_cards": len(equivalent_cards),
            "verified_event_predicate_cards": len(predicate_cards),
            "complete_strategy_minutes": self.state["strategy_complete_minutes"],
            "core_signals_evaluated": self.state["signals_evaluated"],
            "qualified_signals": self.state["qualified_signals"],
            "executable_plans": 0,
        }
        atomic_json(self.root / "status.json", self.state)
        atomic_json(self.report_path, self.state)


def run_observer(args, contract_file, *, on_started=None, on_cycle=None, on_prices=None, fee_config=None):
    if not args.authorize_read:
        raise AuthorizationRequired("ECR_EXPLICIT_CONTINUOUS_READ_REQUIRED")
    if args.duration_seconds < 0:
        raise ValueError("negative duration")
    root = Path(getattr(args, "observer_root", ROOT))
    report = Path(getattr(args, "observer_report", REPORT))
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    lock = (root / "observer.lock").open("a+")
    try:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        lock.close()
        raise AuthorizationRequired("ECR_OBSERVER_ALREADY_RUNNING") from None
    reads = None
    observer = None
    shared_store = None
    stopping = False
    old_handlers = {}
    resources = ExitStack()
    route = None
    public_proxy = None
    deadline = time.monotonic() + args.duration_seconds if args.duration_seconds else None

    def stop(_signum, _frame):
        nonlocal stopping
        stopping = True

    try:
        # Changes only this application's inherited networking environment.
        # Public reads use their explicit Rabisu listener; SIG stays direct.
        resources.enter_context(direct_process_proxy_environment())
        public_route = getattr(args, "public_route", "rabisu-relay")
        if public_route == "rabisu-relay":
            public_proxy = getattr(args, "public_proxy", "socks5h://127.0.0.1:18449")
        elif public_route == "rabisu":
            route = resources.enter_context(
                RabisuProxy(
                    node=getattr(args, "rabisu_node", "Rabisu-LA"),
                    config_path=getattr(args, "rabisu_config", None),
                )
            )
            public_proxy = route.url
        realtime_file = getattr(args, "realtime_contract_file", None)
        realtime_contract, _ = (
            documented_realtime(realtime_file) if realtime_file else documented_realtime()
        )
        reference = refresh_reference(
            Path("reports/strategies/ecr_sig_v1/observer"),
            pinned_spec_path=realtime_contract["openapi_path"],
        )
        reads = ContinuousReads(load_key(args.env_file), contract_file, realtime_file)
        if fee_config is not None and fee_config.namespace == "live":
            shared_store = Store(fee_config.database_url, "live")
        observer = Observer(
            reads,
            PublicReads(reads.private / "external", proxy=public_proxy),
            root=root,
            report=report,
            shared_store=shared_store,
            account_ttl_seconds=fee_config.account_ttl_seconds if fee_config else 45,
        )
        observer.state["public_reference"] = reference
        if public_route == "rabisu-relay":
            observer.state["public_proxy"] = {
                "route": "RABISU_EXISTING_LOCAL_RELAY",
                "listener": public_proxy,
                "existing_service_modified": False,
            }
        for sig in (signal.SIGTERM, signal.SIGINT):
            old_handlers[sig] = signal.signal(sig, stop)

        def start_depth(current):
            # Official identity and the complete catalog already passed. Public
            # discovery and account reporting must not hold the shared SIG feed.
            current.collector = DepthCollector(
                current.tid,
                current.rows,
                lambda: ContinuousReads(load_key(args.env_file), contract_file, realtime_file),
                PublicReads(reads.private / "depth-public", proxy=public_proxy),
                reads.private / "depth",
                expected_account_id=reads.reader.config.expected_account_id,
                realtime_contract_file=realtime_file,
            )
            current.collector.start()

        observer.bootstrap(on_catalog_ready=start_depth)
        fee_approval = getattr(args, "fee_approval_file", None)
        if fee_approval:
            from sigbot.authorization import Approval
            from sigbot.strategies.ecr_sig_v1.automatic import apply_observer_fee

            data = json.loads(Path(fee_approval).read_text())
            data.pop("eligibility_confirmed_by_operator", None)
            apply_observer_fee(observer, fee_config, Approval.model_validate(data))
        if on_started:
            on_started(observer)
        while not stopping and (deadline is None or time.monotonic() < deadline):
            try:
                observer.cycle()
            except (AuthorizationRequired, ContractNotVerified, SchemaMismatch):
                raise
            except SigbotError as exc:
                observer.failures[exc.code] += 1
                observer.state["lifecycle"] = "DEGRADED_READ_RETRY_NO_TRADES"
                observer.publish()
                # Shared 429/backoff applies to every subsequent attempt.
                for _ in range(60):
                    if stopping or deadline is not None and time.monotonic() >= deadline:
                        break
                    time.sleep(1)
                continue
            # Executor failures must propagate; they are not retryable market reads.
            if on_cycle:
                on_cycle(observer)
            # Align actual sampling to 00/20/40 seconds. This permits a completed
            # minute to be evaluated within the strategy's existing 10-second bound.
            for _ in range(max(0, int(20 - time.time() % 20))):
                if stopping or deadline is not None and time.monotonic() >= deadline:
                    break
                time.sleep(1)
                if int(time.monotonic()) % 3 == 0:
                    observer.monitor_prices()
                    if on_prices:
                        on_prices(observer)
                    observer.publish()
        observer.state["lifecycle"] = "STOPPED"
        observer.publish()
        return observer.state
    except (SigbotError, OSError, ValueError, KeyError) as exc:
        if observer:
            observer.state.update(
                lifecycle="FAILED",
                stop_code=exc.code
                if isinstance(exc, SigbotError)
                else "INVALID_RUNTIME_INPUT_OR_STORAGE",
            )
            observer.publish()
        raise
    finally:
        try:
            if observer:
                observer.metadata_stop.set()
                observer.metadata_pool.shutdown(wait=True, cancel_futures=True)
                if observer.collector:
                    observer.collector.close()
        finally:
            try:
                for sig, handler in old_handlers.items():
                    signal.signal(sig, handler)
                if reads:
                    reads.reader.close()
                    reads.key = ""
                if shared_store is not None:
                    shared_store.engine.dispose()
            finally:
                try:
                    resources.close()
                    if observer:
                        if route:
                            observer.state["public_proxy"] = route.evidence
                        if observer.collector:
                            _, _, observer.state["depth_collector"] = observer.collector.snapshot()
                        observer.publish()
                finally:
                    lock.close()
