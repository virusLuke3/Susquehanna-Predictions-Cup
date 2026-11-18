"""Bounded SIG tournament broadcast observation; never an order/account writer.

The SDK-decoded broadcasts are an observation archive, not an execution ledger.
Only POST /realtime/token is allowed, using the shared write request budget.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import time
import traceback
import uuid
from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlsplit

import httpx

from sigbot.errors import (
    AuthorizationRequired,
    ContractNotVerified,
    RateLimited,
    SchemaMismatch,
    SigbotError,
    TransportUnknown,
)
from sigbot.official import OfficialSchema
from sigbot.readcheck import ReadCheck, load_key, private_write
from sigbot.transport import OFFICIAL_ORIGIN

CONTRACT = "contracts/realtime.working.json"


@contextmanager
def direct_process_proxy_environment():
    """SDK v15 otherwise inherits the host proxy. This affects this CLI process only."""
    fields = {"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"}
    saved = {k: os.environ.pop(k) for k in fields if k in os.environ}
    try:
        yield
    finally:
        os.environ.update(saved)


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def instant(value: str) -> Decimal:
    """Compare instants without losing the engine's seventh fractional digit."""
    m = re.fullmatch(r"(.+?)(?:\.(\d+))?(Z|[+-]\d{2}:\d{2})", value)
    if not m:
        raise SchemaMismatch("REALTIME_INVALID_TIMESTAMP")
    try:
        base = datetime.fromisoformat(m[1] + m[3].replace("Z", "+00:00"))
    except ValueError:
        raise SchemaMismatch("REALTIME_INVALID_TIMESTAMP") from None
    return Decimal(int(base.timestamp())) + Decimal("0." + (m[2] or "0"))


def version(book: dict) -> tuple[int, Decimal] | None:
    v = book.get("asOf")
    if v is None:
        return None
    if type(v.get("sequence")) is not int or v["sequence"] < 0:
        raise SchemaMismatch("REALTIME_INVALID_BOOK_VERSION")
    return v["sequence"], instant(v["at"])


def quantity(value, *, price=False) -> str:
    try:
        if isinstance(value, bool):
            raise ValueError
        d = Decimal(str(value))
        if not d.is_finite() or d < 0 or price and d > 1:
            raise ValueError
    except (ValueError, ArithmeticError):
        raise SchemaMismatch("REALTIME_INVALID_BOOK_NUMBER") from None
    return str(d)


class TournamentFeed:
    """Per-market deliveries, per-exchange books, and coalesced REST invalidations."""

    def __init__(self, tournament_id: str, markets: dict[str, list[str]]):
        self.tournament_id = tournament_id
        self.markets = markets
        self.exchange_market: dict[str, str] = {}
        for mid, exchanges in markets.items():
            for eid in exchanges:
                if eid in self.exchange_market:
                    raise SchemaMismatch("REALTIME_EXCHANGE_MAPPING_AMBIGUOUS")
                self.exchange_market[eid] = mid
        self.revisions: dict[str, int] = {}
        self.books: dict[str, dict] = {}
        self.pending = set(markets)
        self.pending_reasons = {mid: {"INITIAL_SUBSCRIPTION"} for mid in markets}
        self.pending_since = dict.fromkeys(markets, now())
        self.expired_books: dict[str, str] = {}
        self.trade_tape_pending: set[str] = set()
        self.dirty_generation = Counter({mid: 1 for mid in markets})
        self.unversioned_generation = dict.fromkeys(markets, 1)
        self.minimum_sequence: dict[str, int] = {}
        self.minimum_expiry_time: dict[str, Decimal] = {}
        self.resynced: set[str] = set()
        self.seen_markets: set[str] = set()
        self.trade_ids: set[tuple[str, str]] = set()
        self.counts: Counter = Counter()
        self.resync_reasons: Counter = Counter({"INITIAL_SUBSCRIPTION": len(markets)})
        self.last_received_at: str | None = None

    def require_market(self, market_id) -> str:
        mid = str(market_id)
        if isinstance(market_id, bool) or mid not in self.markets:
            raise SchemaMismatch("REALTIME_MARKET_OUTSIDE_CATALOG")
        return mid

    def require_scope(self, item: dict, mid: str):
        if "tournamentId" in item and item["tournamentId"] != self.tournament_id:
            raise SchemaMismatch("REALTIME_TOURNAMENT_MISMATCH")
        if "marketId" in item and str(item["marketId"]) != mid:
            raise SchemaMismatch("REALTIME_MARKET_MISMATCH")
        if "exchangeId" in item and str(item["exchangeId"]) not in self.markets[mid]:
            raise SchemaMismatch("REALTIME_EXCHANGE_MISMATCH")

    def invalidate(self, mid: str, reason: str, *, sequence=None, expiry=None, exchange=None):
        self.pending.add(mid)
        self.pending_since.setdefault(mid, now())
        self.pending_reasons.setdefault(mid, set()).add(reason)
        self.dirty_generation[mid] += 1
        if type(sequence) is int and sequence >= 0:
            self.minimum_sequence[mid] = max(sequence, self.minimum_sequence.get(mid, 0))
        elif expiry is not None and exchange in self.markets[mid]:
            self.minimum_expiry_time[exchange] = max(
                instant(expiry), self.minimum_expiry_time.get(exchange, Decimal(0))
            )
        else:
            self.unversioned_generation[mid] = self.dirty_generation[mid]
        self.resync_reasons[reason] += 1

    def invalidate_all(self, reason: str):
        for mid in self.markets:
            self.invalidate(mid, reason)

    def apply_book(self, mid: str, book: dict, source: str):
        self.require_scope(book, mid)
        eid = str(book["exchangeId"])
        if eid not in self.markets[mid]:
            raise SchemaMismatch("REALTIME_EXCHANGE_MISMATCH")
        incoming = version(book)
        old = self.books.get(eid)
        if old is not None:
            held = version(old)
            if held is not None and (incoming is None or incoming <= held):
                self.counts["stale_or_equal_books_ignored"] += 1
                return
        levels = {}
        for side in ("bids", "asks"):
            levels[side] = [
                {"price": quantity(x["price"], price=True), "quantity": quantity(x["quantity"])}
                for x in book[side]
            ]
        expiry = book.get("nextExpiryAt")
        if expiry is not None:
            instant(expiry)
        self.books[eid] = {
            "exchangeId": eid,
            "marketId": mid,
            "asOf": book.get("asOf"),
            "nextExpiryAt": expiry,
            "source": source,
            "received_at": now(),
            "own_orders_included": True,
            **levels,
        }
        if old is not None:
            # Carry the last actual REST validation through newer complete books.
            # A delivery gap still invalidates the market; price/source times stay original.
            for field in (
                "validation_received_at",
                "validation_requested_at",
                "rest_validation_sequence",
            ):
                if field in old:
                    self.books[eid][field] = old[field]
        expired = self.expired_books.get(eid)
        if (
            source == "REALTIME"
            and expired
            and incoming
            and incoming[1] >= instant(expired)
            and (expiry is None or instant(expiry) > instant(now()))
        ):
            self.expired_books.pop(eid)
            if (
                mid in self.resynced
                and self.pending_reasons.get(mid) == {"ORDER_EXPIRY"}
                and not any(e in self.expired_books for e in self.markets[mid])
            ):
                self.pending.discard(mid)
                self.pending_reasons.pop(mid, None)
                self.pending_since.pop(mid, None)
                self.counts["expiry_recovered_by_full_book"] += 1
        self.counts["pushed_books_applied" if source == "REALTIME" else "rest_books_applied"] += 1

    def on_batch(self, mid: str, batch: dict):
        delivery = batch["delivery"]
        revision, previous = delivery["revision"], delivery["previousRevision"]
        if (
            delivery["model"] != "best-effort-authoritative-resync"
            or type(revision) is not int
            or type(previous) is not int
            or not 0 <= previous < revision
        ):
            raise SchemaMismatch("REALTIME_INVALID_DELIVERY")
        # The book can be newer even when the batch's arrays are duplicates.
        for book in batch.get("books", []):
            self.apply_book(mid, book, "REALTIME")
        last = self.revisions.get(mid)
        if batch.get("resyncRequired"):
            self.revisions[mid] = max(last or 0, revision)
            self.trade_tape_pending.add(mid)
            self.invalidate(mid, "RESYNC_REQUIRED", sequence=delivery.get("sourceSequenceThrough"))
            return
        if last is not None and revision <= last:
            self.counts["duplicate_batches_ignored"] += 1
            return
        self.revisions[mid] = revision
        if last is not None and previous > last:
            self.counts["revision_gaps"] += 1
            self.trade_tape_pending.add(mid)
            through = delivery.get("sourceSequenceThrough")
            if (
                mid not in self.pending
                and mid in self.resynced
                and type(through) is int
                and through >= 0
                and all(
                    type(self.books.get(eid, {}).get("rest_validation_sequence")) is int
                    and self.books[eid]["rest_validation_sequence"] >= through
                    for eid in self.markets[mid]
                )
            ):
                # A late batch can describe a gap whose entire engine range
                # was already covered by the actual REST resynchronization.
                # This proves the book, never the missing public trade tape.
                self.counts["revision_gap_already_covered_by_rest"] += 1
                return
            self.invalidate(mid, "REVISION_GAP", sequence=delivery.get("sourceSequenceThrough"))
            return
        for trade in batch["trades"]:
            self.require_scope(trade, mid)
            if trade["sequence"] is None or trade["id"] is None:
                self.trade_tape_pending.add(mid)
                self.invalidate(mid, "UNORDERED_TRADE")
                continue
            key = mid, str(trade["id"])
            if key not in self.trade_ids:
                self.trade_ids.add(key)
                self.counts["unique_public_trades"] += 1
        # Carried full books supersede ordinary bookDirty hints. Settlement always refetches.
        carried = {str(b["exchangeId"]) for b in batch.get("books", [])}
        for hint in batch["bookDirty"]:
            self.require_scope(hint, mid)
            self.counts["batched_book_dirty"] += 1
            if str(hint["exchangeId"]) not in carried:
                self.invalidate(mid, "BOOK_DIRTY_WITHOUT_BOOK")
        for hint in batch["marketSettled"]:
            self.require_scope(hint, mid)
            self.counts["batched_market_settled"] += 1
            self.invalidate(mid, "MARKET_SETTLED")

    def receive(self, event: str, payload: dict):
        self.last_received_at = now()
        self.counts[event] += 1
        if event == "markets_batch":
            for entry in payload["markets"]:
                mid = self.require_market(entry["marketId"])
                self.seen_markets.add(mid)
                self.counts["market_batch_entries"] += 1
                self.on_batch(mid, entry["batch"])
        elif event in {"book_dirty", "market_settled"}:
            mid = self.require_market(payload["marketId"])
            self.require_scope(payload, mid)
            self.invalidate(mid, event.upper())
        else:
            raise SchemaMismatch("REALTIME_UNKNOWN_EVENT")

    def expire(self, at: str):
        current = instant(at)
        for book in self.books.values():
            expiry = book["nextExpiryAt"]
            if expiry and instant(expiry) <= current:
                self.expired_books[book["exchangeId"]] = expiry
                self.invalidate(
                    book["marketId"], "ORDER_EXPIRY", expiry=expiry, exchange=book["exchangeId"]
                )
                book["nextExpiryAt"] = None

    def finish_resync(self, mid: str, generation: int, baselines=None):
        current = self.dirty_generation[mid] == generation
        if baselines is not None:
            # Only actual REST responses establish this proof; a newer cached
            # realtime book cannot stand in for a REST read after a true gap.
            complete = set(baselines) == set(self.markets[mid])
            covers = complete and all(
                value is not None
                and value[0] >= self.minimum_sequence.get(mid, 0)
                and value[1] >= self.minimum_expiry_time.get(eid, Decimal(0))
                for eid, value in baselines.items()
            )
            current = covers and self.unversioned_generation[mid] <= generation
        if current:
            self.resynced.add(mid)
            self.pending.discard(mid)
            self.pending_reasons.pop(mid, None)
            self.pending_since.pop(mid, None)
            self.minimum_sequence.pop(mid, None)
            for eid in self.markets[mid]:
                self.expired_books.pop(eid, None)
                self.minimum_expiry_time.pop(eid, None)
        return current

    def summary(self) -> dict:
        return {
            "catalog_markets": len(self.markets),
            "changed_markets_observed": len(self.seen_markets),
            "changed_market_ids": sorted(self.seen_markets),
            "exchanges_with_books": len(self.books),
            "rest_resynced_markets": len(self.resynced),
            "pending_resync_markets": len(self.pending),
            "pending_book_reasons": dict(
                Counter(reason for reasons in self.pending_reasons.values() for reason in reasons)
            ),
            "expiry_only_pending_markets": sum(
                reasons == {"ORDER_EXPIRY"} for reasons in self.pending_reasons.values()
            ),
            "oldest_pending_seconds": float(
                max(
                    (instant(now()) - instant(at) for at in self.pending_since.values()),
                    default=Decimal(0),
                )
            ),
            "pending_trade_tape_markets": len(self.trade_tape_pending),
            "trade_tape_recovery": "NOT_IMPLEMENTED_IN_BOUNDED_BOOK_DIAGNOSTIC",
            "initial_rest_sync_complete": len(self.resynced) == len(self.markets),
            "all_followed_markets_current": not self.pending and not self.trade_tape_pending,
            "last_received_at": self.last_received_at,
            "event_counts": dict(self.counts),
            "resync_reasons": dict(self.resync_reasons),
        }


def documented_realtime(contract_file=None) -> tuple[dict, OfficialSchema]:
    c = json.loads(Path(contract_file or CONTRACT).read_text())
    for field in ("reference", "openapi"):
        path = Path(c[field + "_path"])
        if (
            not path.is_file()
            or hashlib.sha256(path.read_bytes()).hexdigest() != c[field + "_sha256"]
        ):
            raise ContractNotVerified("REALTIME_EVIDENCE_MISMATCH")
    schema = OfficialSchema(c["openapi_path"])
    description = schema.spec["paths"]["/realtime/token"]["post"]["description"]
    if (
        c["source_url"] != OFFICIAL_ORIGIN + "/api/v1/docs"
        or c["token_path"] != "/api/v1/realtime/token"
        or c["token_permission"] != "AUTH_SESSION"
        or "tournament:{tournament_id}:markets" not in description
        or "markets_batch" not in description
    ):
        raise ContractNotVerified("REALTIME_CHANNEL_NOT_DOCUMENTED")
    return c, schema


def mint_token(key: str, budget, schema: OfficialSchema, *, transport=None) -> tuple[dict, dict]:
    # Authentication-session POST consumes the shared write budget, never the order writer.
    budget.charge_probe_write()
    with httpx.Client(trust_env=False, timeout=15, transport=transport) as http:
        r = http.post(
            OFFICIAL_ORIGIN + "/api/v1/realtime/token", headers={"Authorization": "Bearer " + key}
        )
    receipt = {
        "method": "POST",
        "capability": "realtime_auth",
        "http_status": r.status_code,
        "permission": "AUTH_SESSION",
        "received_at": now(),
        "response_body_saved": False,
    }
    if r.status_code == 429:
        budget.backoff(60)
        raise RateLimited("REALTIME_TOKEN_RATE_LIMITED")
    if r.status_code != 200:
        raise TransportUnknown("REALTIME_TOKEN_REQUEST_REJECTED")
    try:
        body = r.json()
    except ValueError:
        raise SchemaMismatch("REALTIME_TOKEN_RESPONSE_INVALID") from None
    schema.response("/realtime/token", "post", body)
    if not body["token"] or not body["anonKey"]:
        raise SchemaMismatch("REALTIME_TOKEN_MISSING")
    u = urlsplit(body["supabaseUrl"])
    if (
        u.scheme != "https"
        or not u.hostname
        or u.username
        or u.password
        or u.query
        or u.fragment
        or u.path not in {"", "/"}
    ):
        raise SchemaMismatch("REALTIME_CONNECTION_URL_INVALID")
    if instant(body["expiresAt"]) <= instant(now()) + 300:
        raise AuthorizationRequired("REALTIME_TOKEN_EXPIRES_TOO_SOON")
    return body, receipt


async def observe_feed(
    reads, body: dict, feed: TournamentFeed, args, private: Path, *, client_factory=None
) -> dict:
    if client_factory is None:
        from realtime import AsyncRealtimeClient

        client_factory = AsyncRealtimeClient
    for name in ("realtime", "websockets"):
        logger = logging.getLogger(name)
        logger.setLevel(logging.CRITICAL)
        logger.propagate = False
    # Official Supabase protocol: https://supabase.com/docs/guides/realtime/protocol
    client = client_factory(
        body["supabaseUrl"].rstrip("/") + "/realtime/v1",
        token=body["anonKey"],
        auto_reconnect=False,
    )
    await client.set_auth(body["token"])
    topic = f"tournament:{feed.tournament_id}:markets"
    channel = client.channel(topic, {"config": {"private": True}})
    stop = asyncio.Event()
    joined = asyncio.Event()
    state: dict = {
        "channel": topic,
        "subscriptions": 1,
        "subscription_status": "CONNECTING",
        "status_transitions": [],
        "messages": 0,
        "decoded_payload_bytes": 0,
        "socket_closed": False,
        "errors": [],
        "rest_failures": [],
    }
    started = time.monotonic()

    def status_callback(status, err):
        name = getattr(status, "value", str(status))
        state["status_transitions"].append(
            {"at": now(), "status": name, "error_present": err is not None}
        )
        if name == "SUBSCRIBED":
            feed.invalidate_all("SUBSCRIBED_OR_REJOINED")
            state["subscription_status"] = name
            joined.set()
        elif name in {"CHANNEL_ERROR", "TIMED_OUT", "CLOSED"}:
            feed.invalidate_all("SOCKET_OR_CHANNEL_ERROR")
            state["subscription_status"] = name
            state["errors"].append("REALTIME_" + name)
            stop.set()  # No endless retry of a refused join.

    def callback(event):
        def receive(message):
            if stop.is_set():
                state["messages_after_stop_not_archived"] = (
                    state.get("messages_after_stop_not_archived", 0) + 1
                )
                return
            try:
                payload = message["payload"]
                encoded = json.dumps(payload, separators=(",", ":"), default=str).encode()
                state["messages"] += 1
                state["decoded_payload_bytes"] += len(encoded)
                private_write(
                    private / "broadcasts" / f"{state['messages']:06d}.json",
                    json.dumps(
                        {
                            "event": event,
                            "received_at": now(),
                            "representation": "SDK_DECODED_PAYLOAD",
                            "payload": payload,
                        },
                        default=str,
                    ).encode(),
                )
                feed.receive(event, payload)
                if (
                    state["messages"] >= args.max_messages
                    or state["decoded_payload_bytes"] >= args.max_payload_mib * 1024 * 1024
                ):
                    state["stop_reason"] = "MESSAGE_OR_PAYLOAD_BOUND"
                    stop.set()
            except (SigbotError, KeyError, TypeError, ValueError, AttributeError, OSError) as exc:
                state["errors"].append(
                    exc.code
                    if isinstance(exc, SigbotError)
                    else "REALTIME_PAYLOAD_OR_ARCHIVE_INVALID"
                )
                feed.invalidate_all("INVALID_PAYLOAD_OR_ARCHIVE")
                stop.set()

        return receive

    for event in ("markets_batch", "book_dirty", "market_settled"):
        channel.on_broadcast(event, callback(event))

    async def resync():
        periodic = time.monotonic() + 60
        exhausted = False
        while not stop.is_set():
            feed.expire(now())
            if time.monotonic() >= periodic:
                feed.invalidate_all("PERIODIC")
                periodic = time.monotonic() + 60
            if feed.pending and not exhausted:
                mid = min(feed.pending)
                generation = feed.dirty_generation[mid]
                try:
                    baselines = {}
                    for eid in feed.markets[mid]:
                        data = await asyncio.to_thread(
                            reads.call,
                            "orderbook",
                            path_params={"id": eid},
                            query={"tournamentId": feed.tournament_id, "depth": 200},
                        )
                        # Official exchange-book responses do not echo tournamentId.
                        # Scope is the verified explicit query plus the scoped catalog mapping.
                        if data["marketId"] != mid or data["exchangeId"] != eid:
                            raise SchemaMismatch("REALTIME_REST_SCOPE_MISMATCH")
                        feed.apply_book(mid, data, "REST")
                        baselines[eid] = version(data)
                    feed.finish_resync(mid, generation, baselines)
                    feed.counts["rest_market_resyncs"] += 1
                except RateLimited as exc:
                    state["rest_failures"].append(exc.code)
                    exhausted = True  # Continue observing, explicitly retain every pending market.
                except (
                    SigbotError,
                    httpx.HTTPError,
                    KeyError,
                    TypeError,
                    ValueError,
                    AttributeError,
                ) as exc:
                    state["rest_failures"].append(
                        exc.code
                        if isinstance(exc, SigbotError)
                        else "REALTIME_REST_TRANSPORT_ERROR"
                    )
                    state.setdefault("rest_failure_details", []).append(
                        {
                            "market_id": mid,
                            "error_type": type(exc).__name__,
                            "frames": [
                                {"file": Path(f.filename).name, "line": f.lineno}
                                for f in traceback.extract_tb(exc.__traceback__)
                            ],
                        }
                    )
                    stop.set()
            else:
                await asyncio.sleep(0.1)

    worker = None
    try:
        await asyncio.wait_for(channel.subscribe(status_callback), timeout=20)
        waiters = [asyncio.create_task(joined.wait()), asyncio.create_task(stop.wait())]
        try:
            await asyncio.wait(waiters, timeout=15, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for w in waiters:
                w.cancel()
            await asyncio.gather(*waiters, return_exceptions=True)
        if not joined.is_set() and not stop.is_set():
            state["errors"].append("REALTIME_JOIN_NOT_ACKNOWLEDGED")
            stop.set()
        if joined.is_set() and not stop.is_set():
            worker = asyncio.create_task(resync())
            try:
                await asyncio.wait_for(stop.wait(), timeout=args.seconds)
            except asyncio.TimeoutError:
                state["stop_reason"] = "DURATION_BOUND"
                stop.set()
    except Exception as exc:
        # Abort this bounded probe. SDK exception text can contain credential-bearing URLs.
        state["errors"].append(
            exc.code if isinstance(exc, SigbotError) else "REALTIME_CONNECT_FAILED"
        )
    finally:
        stop.set()
        # Let a bounded REST request finish before closing its shared HTTP client.
        try:
            if worker is not None:
                await worker
        finally:
            await client.close()
            state["socket_closed"] = True
    state["observation_seconds"] = round(time.monotonic() - started, 3)
    return state


def run_realtime(args, contract_file: str) -> tuple[dict, int]:
    if not args.authorize_read:
        raise AuthorizationRequired("EXPLICIT_REALTIME_READ_REQUIRED")
    if (
        not 1 <= args.seconds <= 300
        or not 8 <= args.max_requests <= 80
        or not 1 <= args.max_messages <= 1000
        or not 1 <= args.max_payload_mib <= 256
    ):
        raise ValueError("invalid bounded realtime limits")
    output = Path(args.output)
    if output.suffix != ".json" or not output.resolve().is_relative_to(Path("reports").resolve()):
        raise ValueError("realtime output must be a reports JSON file")
    contract, schema = documented_realtime()
    # Dependency check precedes loading any production credential.
    from realtime import AsyncRealtimeClient

    reads = ReadCheck(
        load_key(args.env_file),
        tournament_slug=args.tournament_slug,
        root=Path("runtime/readonly"),
        max_requests=args.max_requests,
        seconds=min(600, args.seconds + 180),
        contract_file=contract_file,
    )
    private = Path("runtime/readonly/realtime/sessions") / uuid.uuid4().hex
    report: dict = {
        "layer": "REAL_READ",
        "scope": "BOUNDED_TOURNAMENT_REALTIME_DIAGNOSTIC",
        "started_at": now(),
        "real_orders": 0,
        "real_cancels": 0,
        "account_modifications": 0,
        "auth_session_post_attempts": 0,
        "production_trading_enabled": False,
        "private_evidence": str(private),
        "official_reference_sha256": contract["reference_sha256"],
        "per_market_channels_subscribed": 0,
        "polymarket_realtime_tested": False,
        "long_running_reconnect_and_token_refresh_tested": False,
        "network_route": "DIRECT_PROCESS_ONLY; MAIN_CLASH_UNCHANGED",
        "rest_scope_evidence": "EXPLICIT_TOURNAMENT_QUERY_AND_SCOPED_CATALOG_IDS; NO_RESPONSE_TOURNAMENT_ID_FIELD",
    }
    try:
        account = reads.call("account_identity")
        reads.reader.config.expected_account_id = account["id"]
        cups, _ = reads.check(
            "tournament_list",
            lambda: reads.pages(
                "tournament_list", query={"status": "any", "limit": 100, "offset": 0}
            ),
        )
        matches = [c for c in cups if c["slug"] == args.tournament_slug]
        if len(matches) != 1:
            raise AuthorizationRequired("REALTIME_TOURNAMENT_NOT_UNIQUE")
        tid = matches[0]["id"]
        reads.reader.config.expected_tournament_id = tid
        reads.reader.config.expected_tournament_slug = args.tournament_slug
        markets, _ = reads.check(
            "market_list",
            lambda: reads.pages(
                "market_list", query={"tournamentId": tid, "status": "any", "limit": 100}
            ),
        )
        if not markets:
            raise SchemaMismatch("REALTIME_EMPTY_CATALOG")
        mapping = {m["id"]: [e["id"] for e in m["exchanges"]] for m in markets}
        report.update(
            tournament_id=tid,
            catalog_complete=True,
            catalog_pages=reads.checks["market_list"]["pages_observed"],
        )
        feed = TournamentFeed(tid, mapping)
        before = reads.budget.count
        try:
            body, receipt = mint_token(reads.key, reads.budget, schema)
        finally:
            report["auth_session_post_attempts"] = reads.budget.count - before
        private_write(private / "token-receipt.json", json.dumps(receipt).encode())
        report["token_receipt"] = receipt
        report["token_expires_at"] = body["expiresAt"]
        with direct_process_proxy_environment():
            state = asyncio.run(
                observe_feed(reads, body, feed, args, private, client_factory=AsyncRealtimeClient)
            )
        report.update(state)
        report.update(feed.summary())
        examples = []
        titles = {m["id"]: m["title"] for m in markets}
        for book in list(feed.books.values())[:6]:
            examples.append(
                {k: v for k, v in book.items() if k not in {"bids", "asks"}}
                | {
                    "title": titles[book["marketId"]],
                    "best_bid": max(book["bids"], key=lambda x: Decimal(x["price"]), default=None),
                    "best_ask": min(book["asks"], key=lambda x: Decimal(x["price"]), default=None),
                    "bid_levels": len(book["bids"]),
                    "ask_levels": len(book["asks"]),
                }
            )
        report["book_examples"] = examples
        private_write(private / "final-books.json", json.dumps(feed.books, default=str).encode())
        report["subscription_acceptance"] = (
            "PASS"
            if any(t["status"] == "SUBSCRIBED" for t in state["status_transitions"])
            else "FAIL"
        )
        report["stream_acceptance"] = (
            "PASS"
            if feed.counts["markets_batch"] and not state["errors"]
            else "FAIL"
            if state["errors"]
            else "NOT_OBSERVED"
        )
        report["status"] = (
            "PASS"
            if report["subscription_acceptance"] == report["stream_acceptance"] == "PASS"
            and not state["rest_failures"]
            else "PARTIAL"
        )
    except Exception as exc:
        # A failed probe is reported as failed; there is no continuation or fake success.
        report.update(
            status="FAIL",
            stop_code=exc.code if isinstance(exc, SigbotError) else "REALTIME_READ_FAILED",
        )
    finally:
        reads.reader.transport.close()
        report["read_and_auth_request_attempts"] = reads.budget.count
        report["rest_get_receipts"] = len(reads.requests)
        report["finished_at"] = now()
        private_write(private / "summary.json", json.dumps(report, default=str, indent=2).encode())
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, default=str, indent=2) + "\n")
    return report, 0 if report["status"] == "PASS" else 2
