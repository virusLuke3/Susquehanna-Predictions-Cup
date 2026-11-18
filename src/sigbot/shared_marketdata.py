"""One credential-bearing SIG collector, read-only consumers and expiring priorities.

The local snapshot retains original engine versions and REST proof times.
Publication or consumption never repairs a gap or refreshes an old book.
"""

import json
import os
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import get_ident

from sigbot.domain import content_hash, utc_time
from sigbot.errors import SchemaMismatch


def atomic(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(path.name + f".{os.getpid()}.{get_ident()}.tmp")
    with temporary.open("w") as output:
        os.chmod(temporary, 0o600)
        json.dump(value, output, default=str)
    temporary.replace(path)


def read_bulk_prices(reads, mapping, tournament):
    expected = {eid: mid for mid, exchanges in mapping.items() for eid in exchanges}
    ids, prices = sorted(expected), {}
    for start in range(0, len(ids), 100):
        requested = ids[start : start + 100]
        data = reads.call(
            "bulk_prices", query={"tournamentId": tournament, "ids": ",".join(requested)}
        )
        returned = [row["exchangeId"] for row in data["data"]]
        missing = data["missingIds"]
        if (
            len(set(returned)) != len(returned)
            or len(set(missing)) != len(missing)
            or set(returned) & set(missing)
            or set(returned) | set(missing) != set(requested)
        ):
            raise SchemaMismatch("SHARED_SIG_BULK_SCOPE_OR_COVERAGE_INVALID")
        received = datetime.now(timezone.utc).isoformat()
        for row in data["data"]:
            if row["marketId"] != expected[row["exchangeId"]]:
                raise SchemaMismatch("SHARED_SIG_BULK_MARKET_MAPPING_INVALID")
            prices[row["exchangeId"]] = {**row, "received_at": received, "source_at": None}
    return prices


def priority_request(path, account, tournament, mapping, markets, timestamp):
    atomic(
        path,
        {
            "account_id": account,
            "tournament_id": tournament,
            "mapping_hash": content_hash(mapping),
            "markets": sorted(set(markets) & set(mapping)),
            "at": timestamp.isoformat(),
            "expires_at": (timestamp + timedelta(seconds=20)).isoformat(),
        },
    )


def read_priority(path, account, tournament, mapping, timestamp):
    try:
        value = json.loads(Path(path).read_text())
    except FileNotFoundError:
        return set()
    if (
        value.get("account_id") != account
        or value.get("tournament_id") != tournament
        or value.get("mapping_hash") != content_hash(mapping)
    ):
        return set()
    if (
        not utc_time(value["at"])
        <= timestamp
        < utc_time(value["expires_at"])
        <= utc_time(value["at"]) + timedelta(seconds=20)
    ):
        return set()
    return set(value["markets"]) & set(mapping)


def read_priorities(path, account, tournament, mapping, timestamp):
    """Union expiring consumer requests without overwriting the MM priority file."""
    path = Path(path)
    markets = read_priority(path, account, tournament, mapping, timestamp)
    stem = path.name.removesuffix(".priority.json")
    for consumer in sorted(path.parent.glob(stem + ".*.priority.json")):
        markets.update(read_priority(consumer, account, tournament, mapping, timestamp))
    return markets


def account_priority_active(path, account, tournament, timestamp):
    try:
        value = json.loads(Path(path).read_text())
    except FileNotFoundError:
        return False
    return bool(
        value.get("account_id") == account
        and value.get("tournament_id") == tournament
        and utc_time(value["at"])
        <= timestamp
        < utc_time(value["expires_at"])
        <= utc_time(value["at"]) + timedelta(seconds=90)
    )


@contextmanager
def account_read_priority(account, tournament):
    source = os.environ.get("SIGBOT_SHARED_SIG_DATA")
    if not source:
        yield
        return
    path = Path(source).with_suffix(".account-read.json")
    stamp = datetime.now(timezone.utc)
    payload = {
        "account_id": account,
        "tournament_id": tournament,
        "at": stamp.isoformat(),
        "expires_at": (stamp + timedelta(seconds=90)).isoformat(),
    }
    atomic(path, payload)
    try:
        yield
    finally:
        atomic(path, {**payload, "expires_at": datetime.now(timezone.utc).isoformat()})


class SharedSigCollector:
    """MM reads the ECR-owned SIG source; never opens an HTTP client or socket."""

    def __init__(self, path, tid, account, rows, *, max_age_seconds=5):
        self.path, self.tid, self.account = Path(path), tid, account
        self.max_age = max_age_seconds
        self.focus = set()
        self.update_catalog(rows)

    def update_catalog(self, rows):
        self.mapping = {
            str(row["api_market_id"]): [str(e) for e in row["exchange_ids"]] for row in rows
        }

    def set_targets(self, contracts, *, market_ids=()):
        if contracts:
            raise ValueError("SIG_CONSUMER_HAS_NO_EXTERNAL_COLLECTION")
        self.focus = set(market_ids) & set(self.mapping)
        priority_request(
            self.path.with_suffix(".priority.json"),
            self.account,
            self.tid,
            self.mapping,
            self.focus,
            datetime.now(timezone.utc),
        )

    def start(self):
        self.set_targets([], market_ids=self.focus)

    def close(self):
        priority_request(
            self.path.with_suffix(".priority.json"),
            self.account,
            self.tid,
            self.mapping,
            (),
            datetime.now(timezone.utc),
        )

    def _read(self, *, focused=False):
        now = datetime.now(timezone.utc)
        self.set_targets([], market_ids=self.focus)
        try:
            envelope = json.loads(self.path.read_text())
        except FileNotFoundError:
            return (
                {},
                {
                    "subscription_status": "SHARED_SOURCE_NOT_READY",
                    "connection_checked_at": None,
                    "last_received_at": None,
                    "pending_reasons": {m: ["SHARED_SOURCE_NOT_READY"] for m in self.mapping},
                    "trade_tape_pending": [],
                    "resynced": [],
                },
                {},
            )
        payload = envelope.get("payload")
        if not isinstance(payload, dict) or envelope.get("digest") != content_hash(payload):
            raise SchemaMismatch("SHARED_SIG_SNAPSHOT_INTEGRITY_FAILED")
        if payload.get("account_id") != self.account or payload.get("tournament_id") != self.tid:
            raise SchemaMismatch("SHARED_SIG_IDENTITY_MISMATCH")
        if payload.get("mapping") != self.mapping:
            raise SchemaMismatch("SHARED_SIG_CATALOG_MISMATCH")
        for eid, book in payload["books"].items():
            if (
                book.get("marketId") not in self.mapping
                or eid not in self.mapping[book["marketId"]]
            ):
                raise SchemaMismatch("SHARED_SIG_BOOK_SCOPE_MISMATCH")
        # json.loads created a private tree for this call. It is never cached or
        # shared with another consumer, so copying all levels again is redundant.
        state = payload["feed_state"]
        books = {
            eid: book
            for eid, book in payload["books"].items()
            if not focused or not self.focus or book["marketId"] in self.focus
        }
        # Evaluate freshness after decoding; parsing is not an upstream heartbeat.
        now = datetime.now(timezone.utc)
        state["public_flow"] = payload.get("public_flow", {})
        if not 0 <= (now - utc_time(payload["published_at"])).total_seconds() <= self.max_age:
            state["subscription_status"] = "SHARED_SOURCE_STALE"
        checked = state.get("connection_checked_at")
        if state["subscription_status"] == "SUBSCRIBED" and (
            checked is None or not 0 <= (now - utc_time(checked)).total_seconds() <= self.max_age
        ):
            state["subscription_status"] = "SHARED_CONNECTION_UNCONFIRMED"
        pending = state.setdefault("pending_reasons", {})
        if state["subscription_status"] != "SUBSCRIBED":
            for mid in self.mapping:
                pending.setdefault(mid, []).append(state["subscription_status"])
        for book in books.values():
            if book.get("nextExpiryAt") and utc_time(book["nextExpiryAt"]) <= now:
                pending.setdefault(book["marketId"], []).append("ORDER_EXPIRY")
        return (
            books,
            state,
            {**payload["summary"], "shared_bulk_prices": payload.get("bulk_prices", {})},
        )

    def prices(self):
        _, state, summary = self._read()
        if state["subscription_status"] in {"SHARED_SOURCE_NOT_READY", "SHARED_SOURCE_STALE"}:
            return {}
        # REST BBO is independently authenticated and timestamped. A missing
        # websocket heartbeat still blocks books, not this monitoring input.
        return {
            str(eid): {
                **row,
                "_read_received_at": row["received_at"],
                "_source": "OFFICIAL_BULK_PRICES_SHARED",
            }
            for eid, row in summary.get("shared_bulk_prices", {}).items()
        }

    def monitoring_snapshot(self):
        books, state, _ = self._read()
        return books, state

    def snapshot(self):
        books, state, summary = self._read(focused=True)
        usable = {
            eid: book
            for eid, book in books.items()
            if book["marketId"] not in state["pending_reasons"]
            and state["subscription_status"] == "SUBSCRIBED"
        }
        result = {
            **summary,
            "method": "SHARED_SIG_AUTHORITATIVE_SOURCE_CONSUMER",
            "subscription_status": state["subscription_status"],
            "consumer_http_requests": 0,
            "consumer_subscriptions": 0,
            "shared_snapshot_path": str(self.path),
            "public_flow": deepcopy(state.get("public_flow", {})),
        }
        result.pop("shared_bulk_prices", None)
        sig = dict(result.get("sig", {}))
        sig.update(
            tracked_markets=len(self.focus),
            monitored_markets=len(self.mapping),
            all_market_books_retained=True,
            tracked_pending_market_ids=sorted(self.focus & set(state["pending_reasons"])),
            tracked_pending_reasons={
                mid: state["pending_reasons"].get(mid, []) for mid in self.focus
            },
            tracked_book_versions={
                eid: {
                    "asOf": b.get("asOf"),
                    "validation_received_at": b.get("validation_received_at"),
                    "source": b.get("source"),
                }
                for eid, b in books.items()
                if b["marketId"] in self.focus
            },
        )
        result["sig"] = sig
        return usable, {}, result
