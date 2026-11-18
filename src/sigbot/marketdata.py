from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from sigbot.domain import ZERO, utc_time
from sigbot.errors import SchemaMismatch


def executable_depth(book: dict, side: str, limit: Decimal) -> Decimal | None:
    levels = book.get("asks" if side == "BUY" else "bids")
    if levels is None or not book.get("own_orders_known"):
        return None
    total = ZERO
    for level in levels:
        if level.get("own"):
            continue
        price, qty = Decimal(level["price"]), Decimal(level["quantity"])
        if not price.is_finite() or not qty.is_finite() or not 0 < price < 1 or qty < 0:
            raise SchemaMismatch("INVALID_BOOK_LEVEL")
        if (side == "BUY" and price <= limit) or (side == "SELL" and price >= limit):
            total += qty
    return total


@dataclass
class RevisionState:
    scope: str
    revision: int
    quality: str = "HEALTHY"
    required_revision: int = 0

    def batch(self, scope: str, previous: int, revision: int) -> None:
        if scope != self.scope or revision <= previous:
            raise SchemaMismatch("REVISION_SCOPE_MISMATCH")
        if revision <= self.revision:
            return
        if previous != self.revision or self.quality == "GAP":
            self.quality = "GAP"
            self.required_revision = max(self.required_revision, revision)
            return
        self.revision = revision

    def resync(self, scope: str, revision: int) -> None:
        if scope != self.scope:
            raise SchemaMismatch("REVISION_SCOPE_MISMATCH")
        if revision < max(self.revision, self.required_revision):
            self.quality = "GAP"
            return
        self.revision, self.quality = revision, "HEALTHY"


def archive(
    raw: bytes,
    folder: str,
    received_at: datetime,
    available_at: datetime,
    source_time: datetime | None = None,
) -> dict:
    utc_time(received_at), utc_time(available_at)
    if available_at < received_at:
        raise SchemaMismatch("INVALID_AVAILABLE_AT")
    digest = hashlib.sha256(raw).hexdigest()
    directory = Path(folder)
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    path = directory / (digest + ".json")
    path.write_bytes(raw)
    path.chmod(0o600)
    record = {
        "sha256": digest,
        "received_at": received_at.isoformat(),
        "available_at": available_at.isoformat(),
        "source_time": source_time.isoformat() if source_time else None,
        "raw_path": str(path),
        "raw_json": raw.decode(),
    }
    import pyarrow as pa
    import pyarrow.parquet as pq

    pq.write_table(pa.Table.from_pylist([record]), directory / (digest + ".parquet"))
    return record


def batch_prices(ids: list[str], fetch, batch_size: int = 100) -> dict:
    if not 1 <= batch_size <= 100 or len(ids) != len(set(ids)):
        raise SchemaMismatch("INVALID_PRICE_BATCH")
    result = {}
    for start in range(0, len(ids), batch_size):
        batch = ids[start : start + batch_size]
        response = fetch(batch)
        if set(response) - set(batch):
            raise SchemaMismatch("UNMAPPED_PRICE_ID")
        for key in batch:
            result[key] = response.get(key)  # absent quote stays unknown
    return result


def parse_raw(raw: bytes) -> dict:
    return json.loads(raw, parse_float=Decimal)


class MarketData:
    """Persistent synthetic revision example; does not assert SIG revision encoding."""

    def __init__(self, store, clock, output_dir):
        self.store, self.clock, self.output_dir = store, clock, output_dir

    def snapshot(self, instrument_id: str, scope: str, revision: int, book: dict) -> str:
        from sqlalchemy import select

        from sigbot.storage import Instrument

        if not instrument_id.startswith("SIM-") or not scope.startswith("SIM-"):
            raise SchemaMismatch("REAL_REVISION_PROTOCOL_UNVERIFIED")
        raw = json.dumps(book, sort_keys=True).encode()
        try:
            evidence = archive(raw, self.output_dir, self.clock.now, self.clock.now)
        except OSError:
            from sigbot.storage import Account

            with self.store.transaction() as s:
                inst = s.get(Instrument, instrument_id)
                if inst:
                    inst.quality = "DEGRADED"
                    for account in s.scalars(
                        select(Account)
                        .where(
                            Account.tournament == inst.tournament,
                            Account.namespace == self.store.namespace,
                        )
                        .with_for_update()
                    ):
                        account.lifecycle, account.reason = "PAUSED", "RAW_ARCHIVE_NOT_WRITABLE"
            raise
        with self.store.transaction() as s:
            inst = s.scalar(
                select(Instrument).where(Instrument.id == instrument_id).with_for_update()
            )
            if inst is None:
                raise SchemaMismatch("INSTRUMENT_UNKNOWN")
            old = inst.book
            if old.get("_scope", scope) != scope:
                raise SchemaMismatch("REVISION_SCOPE_MISMATCH")
            rev = RevisionState(
                scope, old.get("_revision", 0), inst.quality, old.get("_required_revision", 0)
            )
            rev.resync(scope, revision)
            if rev.quality == "HEALTHY":
                inst.book = {
                    **book,
                    "_scope": scope,
                    "_revision": rev.revision,
                    "_required_revision": rev.required_revision,
                    "_raw_hash": evidence["sha256"],
                }
                inst.book_at = self.clock.now
            inst.quality = rev.quality
            return inst.quality

    def batch(self, instrument_id: str, scope: str, previous: int, revision: int) -> str:
        from sqlalchemy import select

        from sigbot.storage import Instrument

        with self.store.transaction() as s:
            inst = s.scalar(
                select(Instrument).where(Instrument.id == instrument_id).with_for_update()
            )
            if inst is None or inst.book.get("_scope") != scope:
                raise SchemaMismatch("REVISION_SCOPE_MISMATCH")
            rev = RevisionState(
                scope, inst.book["_revision"], inst.quality, inst.book.get("_required_revision", 0)
            )
            rev.batch(scope, previous, revision)
            # No unverified price delta is applied. Every accepted batch requires a new full snapshot.
            if rev.quality == "HEALTHY":
                rev.quality = "GAP"
                rev.required_revision = revision
            inst.book = {**inst.book, "_required_revision": rev.required_revision}
            inst.quality = rev.quality
            return rev.quality
