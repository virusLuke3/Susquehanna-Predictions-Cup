"""Full-universe read-only depth collection, independent of strategy admission.

SIG: one private tournament subscription plus rate-budgeted REST repair.
Polymarket: anonymous POST /books for every selected token, never /order.
No HTTP client is shared between the observer, account check and feed worker.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import subprocess
import threading
import time
from collections import Counter
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from urllib.parse import urlsplit

from sigbot.domain import content_hash
from sigbot.errors import AuthorizationRequired, ContractNotVerified, SchemaMismatch, SigbotError
from sigbot.readcheck import private_write
from sigbot.realtime import TournamentFeed, documented_realtime, instant, mint_token, now, version
from sigbot.shared_marketdata import (
    account_priority_active,
    atomic,
    read_bulk_prices,
    read_priorities,
    read_priority,
)
from sigbot.storage_policy import bounded_storage
from sigbot.strategies.ecr_sig_v1.publicdata import public_book

FAILURES = (SigbotError, KeyError, ValueError, TypeError, OSError, subprocess.TimeoutExpired)
PUBLIC_BOOK_BATCH_SIZE = 250  # Local payload bound, not an asserted official maximum.


def contract_binding(c):
    return content_hash(
        {
            k: c.get(k)
            for k in (
                "operator",
                "contract_id",
                "no_token",
                "outcome_side",
                "outcome_tokens",
                "condition_id",
                "market_id",
                "geography",
                "party",
                "title",
                "description",
                "resolution_source",
                "rules_primary",
                "rules_secondary",
            )
        }
    )


def select_resync_market(feed, retry_at, cursor, when, prioritized=()):
    available = sorted(
        (m for m in feed.pending if retry_at.get(m, 0) <= when),
        key=lambda m: (m not in prioritized, m),
    )
    initial = [m for m in available if m not in feed.resynced]
    # Cold markets get their first authoritative baseline before repeatedly
    # repairing busy markets whose short-lived orders keep expiring.
    return initial[0] if initial else available[cursor % len(available)] if available else None


def priority_resync_market(feed, focus, audits, retry_at, cursor, when):
    if cursor % 3:
        return None
    available = sorted(
        mid for mid in focus & (feed.pending | audits) if retry_at.get(mid, 0) <= when
    )
    return available[(cursor // 3) % len(available)] if available else None


def validate_rest_baseline(feed, mid, data, started, received):
    feed.apply_book(mid, data, "REST")
    cached = feed.books[data["exchangeId"]]
    current, baseline = version(cached), version(data)
    if current is None or baseline is None or current < baseline:
        return False
    cached["validation_received_at"] = received
    cached["validation_requested_at"] = started
    cached["rest_validation_sequence"] = baseline[0]
    # A newer complete realtime book can supersede the one-second REST cache.
    return (
        cached["source"] == "REALTIME"
        and instant(cached["received_at"]) >= instant(started)
        and (
            cached.get("nextExpiryAt") is None
            or instant(cached["nextExpiryAt"]) > instant(received)
        )
    )


class DepthCollector:
    def __init__(
        self,
        tid: str,
        rows: list[dict],
        reads_factory,
        public,
        root: Path,
        *,
        expected_account_id=None,
        realtime_contract_file=None,
        sig_only=False,
        rest_audit_seconds=1800,
        monitor_all=False,
        universe_audit_seconds=1800,
    ):
        self.tid, self.reads_factory, self.public, self.root = tid, reads_factory, public, root
        self.expected_account_id = expected_account_id
        self.realtime_contract_file = realtime_contract_file
        self.sig_only = sig_only
        self.monitor_all = monitor_all
        self.universe_audit_seconds = universe_audit_seconds
        self.rest_audit_seconds = rest_audit_seconds
        self.lock = threading.RLock()
        self.stop = threading.Event()
        self.threads: list[threading.Thread] = []
        self.targets: dict[tuple[str, str], dict] = {}
        self.focus: set[str] = set()
        self.audit_pending: set[str] = set()
        self.external: dict[tuple[str, str], dict] = {}
        self.failures: Counter = Counter()
        self.counts: Counter = Counter()
        self.socket_status = "NOT_STARTED"
        self.socket_checked_at: str | None = None
        self.connection_diagnostics: dict = {}
        self.realtime_proxy = os.environ.get("SIGBOT_REALTIME_PROXY") or None
        if self.realtime_proxy:
            proxy = urlsplit(self.realtime_proxy)
            if (
                proxy.scheme not in {"http", "socks5", "socks5h"}
                or proxy.hostname not in {"localhost", "127.0.0.1", "::1"}
                or proxy.port is None
                or proxy.username is not None
                or proxy.password is not None
                or proxy.path not in {"", "/"}
                or proxy.query
                or proxy.fragment
            ):
                raise ValueError("SIG_REALTIME_PROXY_MUST_BE_LOCAL_LISTENER")
        self.current_client: Any = None
        self.shared_output = (
            Path(os.environ["SIGBOT_SHARED_SIG_DATA"])
            if not sig_only and os.environ.get("SIGBOT_SHARED_SIG_DATA")
            else None
        )
        self.priority_focus: set[str] = set()
        self.recovery_focus: set[str] = set()
        self.shared_bulk_prices: dict = {}
        self.shared_public_flow: dict = {}
        self.flow_attempted: dict[str, float] = {}
        self.shared_price_attempts = 0
        self.read_attempts = 0
        self.repair_readers: list = []
        self.token_posts = 0
        self.repair_attempted: dict[str, float] = {}
        self.repair_cursor = 0
        self.round = 0
        self.mapping: dict[str, list[str]] = {}
        self.feed = TournamentFeed(tid, {})
        self.update_catalog(rows)

    def update_catalog(self, rows):
        mapping = {r["api_market_id"]: list(r["exchange_ids"]) for r in rows}
        with self.lock:
            if mapping != self.mapping:
                self.mapping = mapping
                self.shared_bulk_prices = {}
                self.shared_public_flow = {}
                self.flow_attempted = {}
                # ID/exchange changes replace the cache, trigger rejoin and full
                # REST initialization; no old book survives a different scope.
                self.feed = TournamentFeed(self.tid, deepcopy(mapping))
                self.counts["catalog_generations"] += 1

    def set_targets(self, contracts, *, market_ids=()):
        targets = {(c["operator"], c["contract_id"]): deepcopy(c) for c in contracts}
        with self.lock:
            self.focus = set(market_ids) & set(self.mapping)
            self.audit_pending.intersection_update(
                set(self.mapping) if self.monitor_all or self.shared_output else self.focus
            )
            for key in list(self.external):
                if key not in targets or contract_binding(self.targets[key]) != contract_binding(
                    targets[key]
                ):
                    self.external.pop(key)
            self.targets = targets

    def set_bulk_prices(self, prices):
        with self.lock:
            self.shared_bulk_prices = deepcopy(prices)

    def record_error(self, exc, default):
        code = exc.code if isinstance(exc, SigbotError) else default
        with self.lock:
            self.failures[code] += 1

    def schedule_rest_audit(self, feed):
        # A scheduled check does not invalidate a continuous, versioned book.
        # Real delivery gaps, expiry and reconnect still enter feed.pending.
        self.audit_pending.update(self.focus if self.sig_only else feed.markets)

    def schedule_due_audits(self, feed, at):
        scope = self.focus if self.sig_only and not self.monitor_all else feed.markets
        period = self.universe_audit_seconds if self.monitor_all else self.rest_audit_seconds
        for mid in scope:
            if any(
                not feed.books.get(eid, {}).get("validation_received_at")
                or instant(at) - instant(feed.books[eid]["validation_received_at"])
                >= period * 2 / 3
                for eid in feed.markets[mid]
            ):
                # Stagger by actual REST proof time, before the consumer's
                # deadline, rather than queueing the whole universe at expiry.
                self.audit_pending.add(mid)

    def next_repair(self, feed, retry_at, attempted, cursor, when):
        scope = set(feed.markets) if not self.sig_only or self.monitor_all else self.focus
        available = {
            m for m in (feed.pending | self.audit_pending) & scope if retry_at.get(m, 0) <= when
        }
        focus = self.priority_focus if self.shared_output else self.focus
        # MM may prioritize the entire catalog. Preserve a narrow watchdog
        # request in this existing priority turn instead of diluting it in the union.
        priority = (
            (available & self.recovery_focus or available & focus) if cursor % 3 == 0 else set()
        )
        hard = {
            m for m in available & feed.pending if feed.pending_reasons.get(m) != {"ORDER_EXPIRY"}
        }
        # Reserve ordinary turns for audits and expiry repair as well as true
        # gaps. A perpetually busy hard-gap market must not starve the universe.
        audit = available & self.audit_pending
        candidates = (
            priority
            or (audit if cursor % 3 == 1 else set())
            or (available if cursor % 3 == 2 else set())
            or hard
            or available
        )
        return min(candidates, key=lambda m: (attempted.get(m, 0), m), default=None)

    def refresh_public_flow(self, reads):
        """Use the existing shared reader and request budget; one window per turn."""
        from sigbot.history import tape
        from sigbot.public_flow import trade_window

        with self.lock:
            mapping = deepcopy(self.mapping)
            exchanges = {eid: mid for mid in self.priority_focus for eid in mapping.get(mid, [])}
            due = [
                eid for eid in exchanges if time.monotonic() - self.flow_attempted.get(eid, 0) >= 60
            ]
            if not due:
                return
            eid = min(due, key=lambda e: (self.flow_attempted.get(e, 0), e))
            self.flow_attempted[eid] = time.monotonic()
        end = datetime.now(timezone.utc).replace(microsecond=0)
        start = end - timedelta(minutes=10)
        result = tape(reads, exchanges[eid], eid, self.tid, start, end, max_pages=3)
        window = trade_window(result, exchanges[eid], eid, start, end, datetime.now(timezone.utc))
        atomic(
            self.root / "public-trade-windows" / f"{eid}.json",
            {"window": window, "trades": result["trades"]},
        )
        with self.lock:
            if mapping == self.mapping:
                self.shared_public_flow[eid] = window
                self.counts["verified_public_trade_windows"] += (
                    window["status"] == "VERIFIED_PUBLIC_WINDOW"
                )

    def poll_external(self):
        with self.lock:
            targets = deepcopy(self.targets)
        completed: dict = {}
        poly = sorted(k[1] for k in targets if k[0] == "POLYMARKET")
        # Keep the current 204-token universe in one request. Waiting for a
        # second tiny batch made the first batch exceed the unchanged 5s gate.
        for offset in range(0, len(poly), PUBLIC_BOOK_BATCH_SIZE):
            ids = poly[offset : offset + PUBLIC_BOOK_BATCH_SIZE]
            if self.stop.is_set():
                return
            try:
                data = self.public.books(ids)
                receipt = deepcopy(self.public.last_receipt)
                if not isinstance(data, list):
                    raise SchemaMismatch("ECR_BATCH_BOOKS_NOT_ARRAY")
                index = {}
                for book in data:
                    asset = book.get("asset_id")
                    if asset not in ids or asset in index:
                        raise SchemaMismatch("ECR_BATCH_BOOK_ID_DUPLICATE_OR_UNREQUESTED")
                    index[asset] = book
                for token in ids:
                    key = ("POLYMARKET", token)
                    if token not in index:
                        with self.lock:
                            self.failures["ECR_BATCH_BOOK_MISSING_TOKEN"] += 1
                        continue
                    try:
                        completed[key] = public_book(index[token], targets[key], receipt)
                    except FAILURES as exc:
                        self.record_error(exc, "ECR_EXTERNAL_BOOK_INVALID")
            except FAILURES as exc:
                self.record_error(exc, "ECR_BATCH_BOOK_REQUEST_FAILED")
        for key, contract in targets.items():
            if key[0] != "KALSHI" or self.stop.is_set():
                continue
            try:
                data = self.public.get("KALSHI_BOOK", ticker=contract["contract_id"])
                completed[key] = public_book(data, contract, deepcopy(self.public.last_receipt))
            except FAILURES as exc:
                self.record_error(exc, "ECR_KALSHI_BOOK_REQUEST_FAILED")
        with self.lock:
            # Replace the entire round; omitted/failed/changed tokens are gaps,
            # not last-good quotes advertised as this round's results.
            self.external = {
                k: v
                for k, v in completed.items()
                if k in self.targets
                and contract_binding(self.targets[k]) == contract_binding(targets[k])
            }
            self.round += 1
            self.counts["external_rounds"] += 1
            archived = {
                "at": now(),
                "round": self.round,
                "requested": len(targets),
                "returned": len(self.external),
                "books": list(self.external.values()),
            }
        if bounded_storage():
            return
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        # Each round has its own durable snapshot and the public raw receipts.
        private_write(
            self.root / "external-rounds" / f"{self.round:09d}.json",
            json.dumps(archived, default=str).encode(),
        )

    def snapshot(self):
        with self.lock:
            self.feed.expire(now())
            valid = self.socket_status == "SUBSCRIBED"
            books = {
                eid: deepcopy(b)
                for eid, b in self.feed.books.items()
                if valid and b["marketId"] not in self.feed.pending
            }
            return books, deepcopy(self.external), self.summary_locked()

    def monitoring_snapshot(self):
        """Retain pending/stale markets in the denominator, never as usable books."""
        with self.lock:
            self.feed.expire(now())
            return deepcopy(self.feed.books), {
                "subscription_status": self.socket_status,
                "connection_checked_at": self.socket_checked_at,
                "last_received_at": self.feed.last_received_at,
                "pending_reasons": {
                    mid: sorted(reasons) for mid, reasons in self.feed.pending_reasons.items()
                },
                "pending_since": dict(self.feed.pending_since),
                "trade_tape_pending": sorted(self.feed.trade_tape_pending),
                "resynced": sorted(self.feed.resynced),
                "catalog_generation": self.counts["catalog_generations"],
            }

    def summary_locked(self):
        sig = self.feed.summary()
        if self.sig_only:
            sig.update(
                all_market_books_retained=self.monitor_all,
                monitored_markets=len(self.mapping) if self.monitor_all else len(self.focus),
                tracked_markets=len(self.focus),
                tracked_pending_market_ids=sorted(self.feed.pending & self.focus),
                scheduled_rest_audit_market_ids=sorted(self.audit_pending),
                untracked_initialization_not_required=0
                if self.monitor_all
                else len(set(self.mapping) - self.focus),
                tracked_pending_reasons={
                    mid: sorted(self.feed.pending_reasons.get(mid, set())) for mid in self.focus
                },
                tracked_book_versions={
                    eid: {
                        "asOf": book.get("asOf"),
                        "validation_received_at": book.get("validation_received_at"),
                        "nextExpiryAt": book.get("nextExpiryAt"),
                        "source": book.get("source"),
                    }
                    for eid, book in self.feed.books.items()
                    if book["marketId"] in self.focus
                },
            )
        return {
            "method": "SIG_TOURNAMENT_REALTIME_AND_FOCUSED_REST"
            if self.sig_only
            else "SIG_TOURNAMENT_REALTIME_WITH_REST_REPAIR_AND_EXTERNAL_BATCH_REST",
            "channel": f"tournament:{self.tid}:markets",
            "subscriptions": 1,
            "subscription_status": self.socket_status,
            "connection_diagnostics": deepcopy(self.connection_diagnostics),
            "realtime_transport": self.realtime_proxy or "DIRECT",
            "sig": sig,
            "selected_external_tokens": len(self.targets),
            "external_books_last_round": len(self.external),
            "external_round": self.round,
            "external_read_attempts": self.public.count if self.public is not None else 0,
            "sig_read_attempts": self.read_attempts,
            "auth_session_posts": self.token_posts,
            "shared_price_read_attempts": self.shared_price_attempts,
            "recovery_priority_market_ids": sorted(self.recovery_focus),
            "failures": dict(self.failures),
            "counts": dict(self.counts),
            "real_orders": 0,
            "real_cancels": 0,
            "workers_stopped": bool(self.threads) and all(not t.is_alive() for t in self.threads),
        }

    def start(self):
        if self.threads:
            raise ValueError("collector already started")
        # Contract and SDK checks precede production key loading by the factory.
        documented_realtime()
        from realtime import AsyncRealtimeClient  # noqa: F401

        for name, worker in (
            ("sig-depth", self.sig_worker),
            ("external-depth", self.external_worker),
        ):
            if self.sig_only and name == "external-depth":
                continue
            thread = threading.Thread(target=worker, name=name)
            self.threads.append(thread)
            thread.start()
        if self.shared_output:
            thread = threading.Thread(target=self.shared_prices_worker, name="shared-sig-prices")
            self.threads.append(thread)
            thread.start()
            thread = threading.Thread(target=self.shared_worker, name="shared-sig-publisher")
            self.threads.append(thread)
            thread.start()

    def bulk_prices(self):
        with self.lock:
            return deepcopy(self.shared_bulk_prices)

    def shared_prices_worker(self):
        reads = None
        try:
            reads = self.reads_factory()
            account = reads.call("account_identity")
            if account["id"] != self.expected_account_id:
                raise AuthorizationRequired("SHARED_SIG_PRICE_IDENTITY_MISMATCH")
            reads.reader.config.expected_account_id = account["id"]
            reads.reader.config.expected_tournament_id = self.tid
            reads.reader.config.expected_tournament_slug = reads.slug
            while not self.stop.is_set():
                if self.shared_output and account_priority_active(
                    self.shared_output.with_suffix(".account-read.json"),
                    self.expected_account_id,
                    self.tid,
                    datetime.now(timezone.utc),
                ):
                    self.stop.wait(0.2)
                    continue
                started = time.monotonic()
                try:
                    with self.lock:
                        mapping = deepcopy(self.mapping)
                    prices = read_bulk_prices(reads, mapping, self.tid)
                    with self.lock:
                        if mapping == self.mapping:
                            self.shared_bulk_prices = prices
                            self.counts["shared_price_rounds"] += 1
                    try:
                        self.refresh_public_flow(reads)
                    except FAILURES as exc:
                        self.record_error(exc, "SHARED_PUBLIC_TRADE_WINDOW_FAILED")
                except FAILURES as exc:
                    self.record_error(exc, "SHARED_SIG_PRICE_READ_FAILED")
                self.shared_price_attempts = reads.total_attempts
                self.stop.wait(max(1, 15 - (time.monotonic() - started)))
        except FAILURES as exc:
            self.record_error(exc, "SHARED_SIG_PRICE_WORKER_FAILED")
        finally:
            if reads is not None:
                reads.reader.close()
                reads.key = ""

    def shared_worker(self):
        if self.shared_output is None:
            return
        while not self.stop.is_set():
            try:
                with self.lock:
                    self.priority_focus = read_priorities(
                        self.shared_output.with_suffix(".priority.json"),
                        self.expected_account_id,
                        self.tid,
                        self.mapping,
                        datetime.now(timezone.utc),
                    )
                    self.recovery_focus = read_priority(
                        self.shared_output.with_suffix(".lob-recovery.priority.json"),
                        self.expected_account_id,
                        self.tid,
                        self.mapping,
                        datetime.now(timezone.utc),
                    )
                    for mid in self.priority_focus:
                        if any(
                            eid not in self.feed.books
                            or not self.feed.books[eid].get("validation_received_at")
                            or instant(now())
                            - instant(self.feed.books[eid]["validation_received_at"])
                            >= 30
                            for eid in self.mapping[mid]
                        ):
                            self.audit_pending.add(mid)
                    books, state = self.monitoring_snapshot()
                    payload = {
                        "account_id": self.expected_account_id,
                        "tournament_id": self.tid,
                        "mapping": deepcopy(self.mapping),
                        "published_at": now(),
                        "books": books,
                        "feed_state": state,
                        "summary": self.summary_locked(),
                        "bulk_prices": deepcopy(self.shared_bulk_prices),
                        "public_flow": deepcopy(self.shared_public_flow),
                    }
                atomic(self.shared_output, {"payload": payload, "digest": content_hash(payload)})
            except (OSError, ValueError, KeyError, SchemaMismatch) as exc:
                self.record_error(exc, "SHARED_SIG_PUBLICATION_FAILED")
            self.stop.wait(1)

    def close(self):
        self.stop.set()
        for thread in self.threads:
            thread.join(timeout=40)
        if any(t.is_alive() for t in self.threads):
            raise RuntimeError("DEPTH_WORKER_DID_NOT_STOP")

    def external_worker(self):
        while not self.stop.is_set():
            started = time.monotonic()
            try:
                self.poll_external()
            except FAILURES as exc:
                self.record_error(exc, "ECR_EXTERNAL_ARCHIVE_FAILED")
                with self.lock:
                    self.external.clear()
            self.stop.wait(max(0.25, 3 - (time.monotonic() - started)))

    def sig_worker(self):
        try:
            # Independent clients overlap network latency; their file-locked
            # account-wide rolling limit and service backoff remain shared.
            for _ in range(3 if self.shared_output else 1):
                reads = self.reads_factory()
                self.repair_readers.append(reads)
                account = reads.call("account_identity")
                if self.expected_account_id and account["id"] != self.expected_account_id:
                    raise AuthorizationRequired("ECR_DEPTH_ACCOUNT_IDENTITY_MISMATCH")
                reads.reader.config.expected_account_id = account["id"]
                reads.reader.config.expected_tournament_id = self.tid
                reads.reader.config.expected_tournament_slug = reads.slug
            asyncio.run(self.sig_loop(self.repair_readers[0]))
        except Exception as exc:
            # SDK errors may embed JWTs. A worker failure is explicitly degraded;
            # it never publishes old books as valid or returns fake success.
            self.record_error(exc, "ECR_REALTIME_WORKER_FAILED")
            with self.lock:
                self.socket_status = "FAILED"
                self.feed.invalidate_all("WORKER_FAILED")
        finally:
            self.read_attempts = sum(r.total_attempts for r in self.repair_readers)
            for reads in self.repair_readers:
                reads.reader.close()
                reads.key = ""

    async def sig_loop(self, reads):
        _, schema = (
            documented_realtime(self.realtime_contract_file)
            if self.realtime_contract_file
            else documented_realtime()
        )
        while not self.stop.is_set():
            with self.lock:
                feed = self.feed
            try:
                self.token_posts += 1
                reads.renew_budget()
                body, receipt = await asyncio.to_thread(mint_token, reads.key, reads.budget, schema)
                private_write(
                    self.root / f"token-receipt-{self.token_posts:06d}.json",
                    json.dumps(receipt).encode(),
                )
                await self.session(reads, body, feed)
            except Exception as exc:
                self.record_error(exc, "ECR_REALTIME_SESSION_FAILED")
                self.counts["session_exception:" + type(exc).__name__] += 1
            finally:
                with self.lock:
                    self.socket_status = "DISCONNECTED"
                    feed.invalidate_all("DISCONNECTED_OR_TOKEN_REFRESH")
                self.read_attempts = sum(r.total_attempts for r in self.repair_readers)
            # Every join/refresh rebuilds REST baselines, with no blind trade retry.
            for _ in range(20):
                if self.stop.is_set():
                    break
                await asyncio.sleep(0.25)

    async def session(self, reads, body, feed, *, client_factory=None):
        if client_factory is None:
            from realtime import AsyncRealtimeClient
            from websockets.asyncio.client import connect as websocket_connect

            collector = self

            class ObservedRealtimeClient(AsyncRealtimeClient):
                async def connect(self):
                    if self.is_connected:
                        return
                    # One observed heartbeat owns liveness. The native heartbeat
                    # previously disconnected actively delivering subscriptions
                    # on slow control-frame pongs, forcing every book to resync.
                    # check_connection still fails closed after 5s without proof
                    # and reconnects after 60s. TLS/authentication and subscribed
                    # scope remain identical through an explicit existing proxy.
                    self._ws_connection = await websocket_connect(
                        self.url, ping_interval=None, proxy=collector.realtime_proxy
                    )
                    await self._on_connect()

                async def _on_connect_error(self, exc):
                    # The SDK exception text can include authenticated URLs.
                    # Keep only exception types and numeric websocket codes.
                    with collector.lock:
                        collector.counts["socket_error_type:" + type(exc).__name__] += 1
                        code = getattr(exc, "code", None)
                        if isinstance(code, int) and not isinstance(code, bool):
                            collector.counts[f"socket_error_code:{int(code)}"] += 1
                        if "ping timeout" in getattr(getattr(exc, "sent", None), "reason", ""):
                            collector.counts["socket_keepalive_ping_timeout"] += 1
                        for part in ("rcvd", "sent"):
                            code = getattr(getattr(exc, part, None), "code", None)
                            if isinstance(code, int) and not isinstance(code, bool):
                                collector.counts[f"socket_{part}_code:{int(code)}"] += 1
                    await super()._on_connect_error(exc)

            client_factory = ObservedRealtimeClient
        for name in ("realtime", "websockets"):
            logger = logging.getLogger(name)
            logger.setLevel(logging.CRITICAL)
            logger.propagate = False
        client = client_factory(
            body["supabaseUrl"].rstrip("/") + "/realtime/v1",
            token=body["anonKey"],
            auto_reconnect=False,
        )
        with self.lock:
            self.current_client = client
        await client.set_auth(body["token"])
        channel = client.channel(f"tournament:{self.tid}:markets", {"config": {"private": True}})
        joined, failed = asyncio.Event(), asyncio.Event()
        heartbeat_pending: dict[str, float] = {}
        protocol_heartbeat = (
            hasattr(client, "channels") and hasattr(client, "send") and hasattr(client, "_make_ref")
        )
        if protocol_heartbeat:
            from realtime.message import Message
            from realtime.types import PHOENIX_CHANNEL, ChannelEvents

            def heartbeat_reply(message):
                if (
                    joined.is_set()
                    and not failed.is_set()
                    and not self.stop.is_set()
                    and self.current_client is client
                    and client.is_connected
                    and client._ws_connection.close_code is None
                    and message.topic == PHOENIX_CHANNEL
                    and message.event == ChannelEvents.reply
                    and message.ref in heartbeat_pending
                    and message.payload.status == "ok"
                ):
                    sent = heartbeat_pending.pop(message.ref)
                    with self.lock:
                        self.socket_checked_at = now()
                        self.socket_status = "SUBSCRIBED"
                        self.counts["application_heartbeats_confirmed"] += 1
                        self.connection_diagnostics.update(
                            heartbeat_confirmed_at=self.socket_checked_at,
                            heartbeat_roundtrip_seconds=time.monotonic() - sent,
                            heartbeat_pending=len(heartbeat_pending),
                        )

            # The installed SDK dispatches validated replies by their topic.
            # Observe its existing protocol through the same authenticated socket.
            client.channels[PHOENIX_CHANNEL] = SimpleNamespace(
                topic=PHOENIX_CHANNEL,
                is_joined=False,
                is_joining=False,
                _joined_once=False,
                _handle_message=heartbeat_reply,
            )

        def status_callback(status, err):
            name = getattr(status, "value", str(status))
            with self.lock:
                self.socket_status = name
                if name == "SUBSCRIBED":
                    feed.invalidate_all("SUBSCRIBED_OR_REJOINED")
                    self.socket_checked_at = now()  # Actual acknowledged join.
                    joined.set()
                elif name in {"CHANNEL_ERROR", "TIMED_OUT", "CLOSED"}:
                    feed.invalidate_all("SOCKET_OR_CHANNEL_ERROR")
                    failed.set()
                    if (
                        name == "CHANNEL_ERROR"
                        and err is not None
                        and any(text in str(err) for text in ("Unauthorized", "Too many channels"))
                    ):
                        self.failures["ECR_REALTIME_JOIN_REFUSED"] += 1
                        self.stop.set()  # Needs credentials/channel-count repair, not retrying.

        loop = asyncio.get_running_loop()
        previous_handler = loop.get_exception_handler()

        def sdk_background_failure(_loop, _context):
            # Never print SDK exception/URL/message context, which may carry a JWT.
            with self.lock:
                self.failures["ECR_REALTIME_BACKGROUND_TASK_FAILED"] += 1
                feed.invalidate_all("SDK_BACKGROUND_TASK_FAILED")
            failed.set()

        loop.set_exception_handler(sdk_background_failure)

        def callback(event):
            def receive(message):
                try:
                    payload = message["payload"]
                    with self.lock:
                        feed.receive(event, payload)
                        self.socket_checked_at = now()  # Actual scoped server message.
                        if not failed.is_set():
                            self.socket_status = "SUBSCRIBED"
                        archive = payload
                        if self.sig_only and not self.monitor_all:
                            feed.books = {
                                eid: book
                                for eid, book in feed.books.items()
                                if book["marketId"] in self.focus
                            }
                            if event == "markets_batch":
                                archive = {
                                    **payload,
                                    "markets": [
                                        entry
                                        for entry in payload["markets"]
                                        if str(entry["marketId"]) in self.focus
                                    ],
                                }
                            elif str(payload.get("marketId")) not in self.focus:
                                archive = None
                        self.counts["broadcasts"] += 1
                        sequence = self.counts["broadcasts"]
                    if bounded_storage() or archive is None or event == "markets_batch" and not archive["markets"]:
                        return
                    private_write(
                        self.root / "broadcasts" / f"{sequence:09d}.json",
                        json.dumps(
                            {"event": event, "received_at": now(), "payload": archive}, default=str
                        ).encode(),
                    )
                except FAILURES as exc:
                    self.record_error(exc, "ECR_REALTIME_PAYLOAD_OR_ARCHIVE_INVALID")
                    with self.lock:
                        feed.invalidate_all("INVALID_PAYLOAD_OR_ARCHIVE")
                    failed.set()

            return receive

        for event in ("markets_batch", "book_dirty", "market_settled"):
            channel.on_broadcast(event, callback(event))
        retry_at: dict[str, float] = {}
        cursor = self.repair_cursor
        attempted = self.repair_attempted
        audit_at = 0.0

        async def check_connection():
            # REST can wait on the shared budget or HTTP. It must not make an
            # otherwise live subscription look dead to every strategy.
            pong = None
            next_heartbeat = 0.0
            while not self.stop.is_set() and not failed.is_set():
                ws = getattr(client, "_ws_connection", None)
                if not client.is_connected or ws is not None and ws.close_code is not None:
                    with self.lock:
                        self.socket_status = "DISCONNECTED"
                        feed.invalidate_all("SOCKET_DISCONNECTED")
                        self.counts["session_socket_disconnected"] += 1
                        code = getattr(ws, "close_code", None)
                        if type(code) is int:
                            self.counts[f"socket_close_code:{code}"] += 1
                    failed.set()
                    break
                if instant(body["expiresAt"]) <= instant(now()) + 30:
                    self.counts["session_token_refresh_due"] += 1
                    failed.set()
                    break
                if ws is not None:
                    age = (
                        instant(now()) - instant(self.socket_checked_at)
                        if self.socket_checked_at
                        else Decimal("Infinity")
                    )
                    if age > 60:
                        with self.lock:
                            self.socket_status = "DISCONNECTED"
                            feed.invalidate_all("CONNECTION_PROOF_TIMEOUT")
                            self.counts["connection_proof_timeout"] += 1
                        failed.set()
                        break
                    if protocol_heartbeat:
                        if age > 5:
                            with self.lock:
                                self.socket_status = "PING_UNCONFIRMED"
                        monotonic = time.monotonic()
                        for ref, sent in list(heartbeat_pending.items()):
                            if monotonic - sent >= 60:
                                del heartbeat_pending[ref]
                                self.counts["application_heartbeats_expired"] += 1
                        if monotonic >= next_heartbeat:
                            # One missed reply must not suppress every later
                            # proof. Only a matching real reply renews liveness.
                            heartbeat_ref = client._make_ref()
                            heartbeat_pending[heartbeat_ref] = monotonic
                            next_heartbeat = monotonic + 2
                            await client.send(
                                Message(
                                    topic=PHOENIX_CHANNEL,
                                    event=ChannelEvents.heartbeat,
                                    payload={},
                                    ref=heartbeat_ref,
                                )
                            )
                            with self.lock:
                                self.counts["application_heartbeats_sent"] += 1
                                self.connection_diagnostics.update(
                                    heartbeat_sent_at=now(),
                                    heartbeat_send_seconds=time.monotonic() - monotonic,
                                    heartbeat_pending=len(heartbeat_pending),
                                )
                        await asyncio.sleep(0.25)
                        continue
                    if age <= 2 and pong is None:
                        # A scoped server delivery is already a real proof.
                        await asyncio.sleep(0.25)
                        continue
                    try:
                        if pong is None:
                            pong = await ws.ping()
                        await asyncio.wait_for(asyncio.shield(pong), timeout=3)
                    except asyncio.TimeoutError:
                        with self.lock:
                            self.counts["connection_ping_unconfirmed"] += 1
                            if (
                                not self.socket_checked_at
                                or instant(now()) - instant(self.socket_checked_at) > 5
                            ):
                                self.socket_status = "PING_UNCONFIRMED"
                    else:
                        pong = None
                        with self.lock:
                            if not failed.is_set():
                                self.socket_checked_at = now()
                                self.socket_status = "SUBSCRIBED"
                    await asyncio.sleep(0.25)
                else:
                    # Socket existence is not a receipt. Only join/broadcast/pong
                    # callbacks can refresh the connection proof.
                    await asyncio.sleep(0.25)

        connection_task = None
        repairs: dict[int, tuple[str, asyncio.Task]] = {}

        async def repair(mid, generation, reader):
            try:
                with self.lock:
                    reasons = sorted(feed.pending_reasons.get(mid, {"PERIODIC_AUDIT"}))
                    for reason in reasons:
                        self.counts[f"rest_attempt_reason:{reason}"] += 1
                rest_baselines = {}
                for eid in feed.markets[mid]:
                    if self.stop.is_set():
                        break
                    started = now()
                    data = await asyncio.to_thread(
                        reader.call,
                        "orderbook",
                        path_params={"id": eid},
                        query={"tournamentId": self.tid, "depth": 200},
                    )
                    if data["marketId"] != mid or data["exchangeId"] != eid:
                        raise SchemaMismatch("ECR_REALTIME_REST_SCOPE_MISMATCH")
                    with self.lock:
                        validate_rest_baseline(feed, mid, data, started, now())
                        rest_baselines[eid] = version(data)
                if not self.stop.is_set():
                    with self.lock:
                        if feed.finish_resync(mid, generation, rest_baselines):
                            self.audit_pending.discard(mid)
                            self.counts["rest_resyncs"] += 1
                        else:
                            self.counts["rest_resyncs_pending_newer_proof"] += 1
            except (AuthorizationRequired, ContractNotVerified) as exc:
                self.record_error(exc, "ECR_SIG_REST_IDENTITY_OR_CONTRACT_FAILED")
                self.stop.set()
            except FAILURES as exc:
                self.record_error(exc, "ECR_SIG_REST_REPAIR_FAILED")
                retry_at[mid] = time.monotonic() + 30

        try:
            await asyncio.wait_for(channel.subscribe(status_callback), timeout=20)
            await asyncio.wait_for(joined.wait(), timeout=15)
            connection_task = asyncio.create_task(check_connection())
            while not self.stop.is_set() and not failed.is_set():
                if connection_task.done():
                    connection_task.result()
                for key, (mid, task) in list(repairs.items()):
                    if task.done():
                        del repairs[key]
                        if retry_at.get(mid) == float("inf"):
                            retry_at.pop(mid)
                        task.result()
                if self.shared_output and account_priority_active(
                    self.shared_output.with_suffix(".account-read.json"),
                    self.expected_account_id,
                    self.tid,
                    datetime.now(timezone.utc),
                ):
                    # Account reconciliation owns the next read turns. Market
                    # broadcasts and their original continuity checks continue.
                    await asyncio.sleep(0.1)
                    continue
                with self.lock:
                    if self.feed is not feed:
                        break
                    feed.expire(now())
                    if time.monotonic() >= audit_at:
                        self.schedule_due_audits(feed, now())
                        audit_at = time.monotonic() + 10
                readers = self.repair_readers or [reads]
                with self.lock:
                    for reader in readers:
                        if id(reader) in repairs:
                            continue
                        mid = self.next_repair(feed, retry_at, attempted, cursor, time.monotonic())
                        cursor += 1
                        self.repair_cursor = cursor
                        if mid is None:
                            break
                        if mid in self.recovery_focus and (cursor - 1) % 3 == 0:
                            self.counts["recovery_priority_repairs_started"] += 1
                        attempted[mid] = time.monotonic()
                        retry_at[mid] = float("inf")  # One in-flight repair per market.
                        repairs[id(reader)] = (
                            mid,
                            asyncio.create_task(repair(mid, feed.dirty_generation[mid], reader)),
                        )
                self.read_attempts = sum(r.total_attempts for r in readers)
                await asyncio.sleep(0.1)
        finally:
            # Finish real in-flight I/O before changing the session generation.
            # Each free client can progress without waiting for the slowest one.
            results = await asyncio.gather(
                *(task for _, task in repairs.values()), return_exceptions=True
            )
            for mid, _ in repairs.values():
                if retry_at.get(mid) == float("inf"):
                    retry_at.pop(mid)
            self.read_attempts = sum(r.total_attempts for r in (self.repair_readers or [reads]))
            if connection_task is not None:
                connection_task.cancel()
                await asyncio.gather(connection_task, return_exceptions=True)
            with self.lock:
                feed.invalidate_all("SESSION_CLOSED")
                self.socket_status = "DISCONNECTED"
                self.current_client = None
            try:
                await client.close()
            finally:
                loop.set_exception_handler(previous_handler)
            for result in results:
                if isinstance(result, BaseException):
                    raise result
