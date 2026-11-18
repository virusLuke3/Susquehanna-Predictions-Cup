"""Receipt-time datasets from existing archives; no HTTP client or invented history."""

from __future__ import annotations

import gzip
import hashlib
import json
import math
import os
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from sigbot.domain import content_hash, utc_time
from sigbot.errors import SchemaMismatch
from sigbot.readcheck import private_write
from sigbot.shared_marketdata import priority_request
from sigbot.strategies.semantic_sig_v1.registry import write_json


def metadata(path):
    raw = Path(path).read_bytes()
    registry = json.loads(raw)
    markets = registry["markets"]
    selected = {}
    for row in markets:
        details = row.get("official_contract_details") or {}
        if row.get("scope") != "RACE":
            continue
        if any(not details.get(k) for k in ("raceId", "stageId", "electionDate", "winnerName")):
            raise SchemaMismatch("SEMANTIC_OFFICIAL_EVENT_ID_REQUIRED")
        eid = row["exchange_id"]
        if not eid or eid in selected:
            raise SchemaMismatch("SEMANTIC_DATASET_EXCHANGE_ID_AMBIGUOUS")
        selected[eid] = {
            "market_id": row["sig_market_id"],
            "local_key": row["local_market_key"],
            "race_id": row["race_id"],
            "event_id": content_hash([details[k] for k in ("raceId", "stageId", "electionDate")]),
            "state": details["usState"],
            "party": row["party_code"],
            "rules_hash": row["rules_hash"],
            "contract_details": details,
        }
    return registry["tournament_id"], selected, hashlib.sha256(raw).hexdigest()


def capture(source, output, *, seconds=60, interval=2, market_ids=()):
    """Bounded passive observation. A fresh publisher never repairs a stale feed."""
    if not 0 < seconds <= 3600 or not 0 < interval <= 10:
        raise ValueError("SEMANTIC_CAPTURE_BOUND_INVALID")
    root = Path(output)
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    samples = []
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        stamp = datetime.now(timezone.utc)
        raw = Path(source).read_bytes()
        envelope = json.loads(raw)
        payload = envelope["payload"]
        if envelope.get("digest") != content_hash(payload):
            raise SchemaMismatch("SHARED_SIG_SNAPSHOT_INTEGRITY_FAILED")
        if set(market_ids) - set(payload["mapping"]):
            raise SchemaMismatch("SEMANTIC_PRIORITY_MARKET_OUTSIDE_SCOPE")
        if market_ids:
            priority_request(
                Path(source).with_suffix(".semantic.priority.json"),
                payload["account_id"],
                payload["tournament_id"],
                payload["mapping"],
                market_ids,
                stamp,
            )
        state, books = payload["feed_state"], payload["books"]
        age = (stamp - utc_time(payload["published_at"])).total_seconds()
        checked = state.get("connection_checked_at")
        proof_age = (stamp - utc_time(checked)).total_seconds() if checked else None
        healthy_connection = (
            state["subscription_status"] == "SUBSCRIBED"
            and 0 <= age <= 5
            and proof_age is not None
            and 0 <= proof_age <= 5
        )
        pending = set(state["pending_reasons"])
        for book in books.values():
            if book.get("nextExpiryAt") and utc_time(book["nextExpiryAt"]) <= stamp:
                pending.add(book["marketId"])
        path = root / f"{stamp.strftime('%Y%m%dT%H%M%S%fZ')}.json"
        private_write(path, raw)
        samples.append(
            {
                "observed_at": stamp.isoformat(),
                "published_at": payload["published_at"],
                "raw_path": str(path),
                "raw_sha256": hashlib.sha256(raw).hexdigest(),
                "mapping_hash": content_hash(payload["mapping"]),
                "subscription_status": state["subscription_status"],
                "connection_proof_age_seconds": proof_age,
                "publication_age_seconds": age,
                "healthy_connection": healthy_connection,
                "market_count": len(payload["mapping"]),
                "books": len(books),
                "healthy_markets": len(set(payload["mapping"]) - pending)
                if healthy_connection
                else 0,
                "pending_markets": len(pending),
                "pending_reasons": dict(
                    Counter(r for rs in state["pending_reasons"].values() for r in rs)
                ),
                "collector_counts": payload["summary"].get("counts", {}),
            }
        )
        time.sleep(min(interval, max(0, deadline - time.monotonic())))
    result = {
        "layer": "REAL_READ_LOCAL_SOURCE",
        "http_requests": 0,
        "real_writes": 0,
        "synthetic_data_used": False,
        "samples": samples,
        "all_samples_healthy_connection": all(s["healthy_connection"] for s in samples),
        "all_markets_contiguous": all(s["healthy_markets"] == s["market_count"] for s in samples),
        "scope": "BOUNDED_WINDOW_NOT_CONTINUOUS_SERVICE_ACCEPTANCE",
    }
    write_json(root / "source_health.json", result)
    return result


def build_dataset(source, registry, output, *, cadence=60, max_age=20, max_bytes=None):
    """Freeze a prefix before reading a growing archive; stream once, no future fill.

    The archive's observation time is the conservative availability clock. Its
    original receipt clock is kept separately. BBO can support exploratory
    forecasts, but aggregate prices cannot prove third-party execution capacity.
    """
    if cadence <= 0 or max_age <= 0 or max_age > cadence:
        raise ValueError("SEMANTIC_GRID_OR_ASOF_BOUND_INVALID")
    source, root = Path(source), Path(output)
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    tournament, instruments, registry_hash = metadata(registry)
    source_size = source.stat().st_size
    limit = source_size if max_bytes is None else min(source_size, max_bytes)
    if limit <= 0:
        raise ValueError("SEMANTIC_EMPTY_ARCHIVE")
    digest = hashlib.sha256()
    latest: dict[str, dict] = {}
    counts: Counter[str] = Counter()
    next_tick, last_available = None, None
    frame_path = root / "frames.jsonl.gz"
    temporary = frame_path.with_suffix(".tmp")
    frames, first_tick, final_tick = 0, None, None
    first_available, final_available = None, None
    seen_versions: dict[str, str] = {}
    with source.open("rb") as stream, gzip.open(temporary, "wt") as target:
        os.chmod(temporary, 0o600)
        while stream.tell() < limit:
            offset = stream.tell()
            raw = stream.readline(limit - offset)
            digest.update(raw)
            if not raw.endswith(b"\n"):
                counts["truncated_tail_not_used"] += 1
                break
            counts["archive_rows"] += 1
            packet = json.loads(raw)
            available = utc_time(packet["at"]).timestamp()
            if last_available is not None and available < last_available:
                counts["out_of_order_archive_rows_not_used"] += 1
                continue
            if next_tick is None:
                next_tick = (math.floor(available / cadence) + 1) * cadence
            while next_tick <= available:
                quotes = {
                    eid: q
                    for eid, q in latest.items()
                    if 0 <= next_tick - q["receipt_epoch"] <= max_age
                    and q["available_epoch"] <= next_tick
                }
                target.write(json.dumps({"at": next_tick, "quotes": quotes}) + "\n")
                frames += 1
                first_tick = next_tick if first_tick is None else first_tick
                final_tick = next_tick
                next_tick += cadence
            row_hash = hashlib.sha256(raw).hexdigest()
            for eid, quote in packet["quotes"].items():
                if eid not in instruments:
                    continue
                if (str(quote["marketId"]), str(quote["exchangeId"]), quote.get("option")) != (
                    instruments[eid]["market_id"],
                    eid,
                    "YES",
                ):
                    raise SchemaMismatch("SEMANTIC_ARCHIVE_MARKET_EXCHANGE_OR_SIDE_CHANGED")
                received = utc_time(quote["received_at"]).timestamp()
                if received > available:
                    raise SchemaMismatch("SEMANTIC_ARCHIVE_RECEIPT_AFTER_AVAILABILITY")
                version = content_hash([eid, received, quote.get("bestBid"), quote.get("bestAsk")])
                if seen_versions.get(eid) == version:
                    counts["duplicate_receipts_not_new_samples"] += 1
                    continue
                seen_versions[eid] = version
                counts["unique_quote_receipts"] += 1
                bid, ask = quote.get("bestBid"), quote.get("bestAsk")
                if bid is None or ask is None:
                    latest.pop(eid, None)
                    counts["missing_bbo"] += 1
                    continue
                b, a = float(bid), float(ask)
                if not math.isfinite(b + a) or not 0 <= b < a <= 1:
                    latest.pop(eid, None)
                    counts["invalid_or_crossed_bbo"] += 1
                    continue
                latest[eid] = {
                    "bid": str(bid),
                    "ask": str(ask),
                    "receipt_epoch": received,
                    "received_at": quote["received_at"],
                    "available_epoch": available,
                    "available_at": packet["at"],
                    "source_at": quote.get("source_at"),
                    "archive_byte_offset": offset,
                    "archive_row_sha256": row_hash,
                    "version": version,
                    "own_orders_known": False,
                    "depth": None,
                }
            first_available = available if first_available is None else first_available
            final_available, last_available = available, available
    temporary.replace(frame_path)
    result = {
        "layer": "REAL_READ_ARCHIVE_RECONSTRUCTION",
        "data_provenance": "EXISTING_OFFICIAL_REST_QUOTE_OBSERVATION_ARCHIVE",
        "synthetic_data_used": any(eid.startswith("SIM-") for eid in instruments),
        "tournament_id": tournament,
        "source_path": str(source),
        "source_prefix_bytes": limit,
        "source_size_at_start": source_size,
        "source_prefix_sha256": digest.hexdigest(),
        "source_tail_omitted": limit < source_size,
        "registry_sha256": registry_hash,
        "frames_path": str(frame_path),
        "frames_sha256": hashlib.sha256(frame_path.read_bytes()).hexdigest(),
        "cadence_seconds": cadence,
        "asof_max_age_seconds": max_age,
        "frames": frames,
        "first_grid_epoch": first_tick,
        "last_grid_epoch": final_tick,
        "first_archive_availability_epoch": first_available,
        "last_archive_availability_epoch": final_available,
        "instruments": instruments,
        "counts": dict(counts),
        "source_timestamp_provided": False,
        "synchronous_l2_available": False,
        "historical_own_order_exclusion": "NOT_VERIFIED",
        "historical_rules_pit": "NOT_VERIFIED",
        "executable_capacity_verified": False,
        "real_orders": 0,
    }
    write_json(root / "dataset_manifest.json", result)
    return result


def frames(manifest):
    path = Path(manifest["frames_path"])
    if hashlib.sha256(path.read_bytes()).hexdigest() != manifest["frames_sha256"]:
        raise SchemaMismatch("SEMANTIC_DATASET_HASH_CHANGED")
    with gzip.open(path, "rt") as stream:
        previous = None
        for line in stream:
            frame = json.loads(line)
            if previous is not None and frame["at"] <= previous:
                raise SchemaMismatch("SEMANTIC_DATASET_TIME_NOT_INCREASING")
            previous = frame["at"]
            yield frame
