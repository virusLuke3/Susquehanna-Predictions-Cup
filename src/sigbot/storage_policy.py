"""Optional bounded market storage; financial evidence is never filtered here."""

import fcntl
import hashlib
import os
import re
import tempfile
import time
from datetime import timedelta
from pathlib import Path

MARKET_READS = frozenset({
    "bulk_prices", "orderbook", "price_history", "public_trades", "market_list", "market_nodes",
})
MARKET_AUDITS = (
    "MM13_OBSERVATION", "MM132_BOOK_SAMPLE", "MM_REFERENCE",
    "MM131_SIDE_DECISIONS", "MM13_UNIVERSE",
)
MARKET_JOURNALS = frozenset({
    "quotes.jsonl", "book-observations.jsonl", "price-minutes.jsonl",
    "minute-coverage.jsonl", "cross-market-price-changes.jsonl",
})
FINANCIAL_POLLS = frozenset({
    "open_orders", "fills", "order_fills", "transactions", "positions",
})
POLL_CACHE_SECONDS = 600
_last_cache_prune = {}


def transient_financial_read(name, status):
    return bounded_storage() and name in FINANCIAL_POLLS and 200 <= status < 300


def save_read_evidence(path, name, status, raw):
    """Keep write/error evidence durable; successful polls are a short-lived cache.

    PostgreSQL holds the financial events and recovery checkpoints. Cache files
    remain content addressed during a complete paginated read, so a subsequent
    page cannot overwrite an earlier page's receipt. No background job is needed.
    """
    from sigbot.readcheck import private_write

    if not retain_read(name):
        return None
    path = Path(path)
    if not transient_financial_read(name, status):
        if not path.exists():
            private_write(path, raw)
        return path
    directory = Path(os.environ.get("SIGBOT_CURRENT_READ_DIR", path.parent / "current"))
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    directory.chmod(0o700)
    digest = hashlib.sha256(raw).hexdigest()
    path = directory / f"{name}-{digest}.json"
    with (directory / ".cache.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if not path.exists():
            fd, temporary = tempfile.mkstemp(dir=directory, prefix=".read-")
            try:
                with os.fdopen(fd, "wb") as handle:
                    handle.write(raw)
                os.replace(temporary, path)
            finally:
                Path(temporary).unlink(missing_ok=True)
        else:
            os.utime(path, None)
        now = time.time()
        if now - _last_cache_prune.get(str(directory), 0) >= 60:
            for old in directory.iterdir():
                if not re.fullmatch(r"(?:" + "|".join(FINANCIAL_POLLS) + r")-[a-f0-9]{64}\.json", old.name):
                    continue
                if old.is_file() and not old.is_symlink() and now - old.stat().st_mtime > POLL_CACHE_SECONDS:
                    old.unlink()
            _last_cache_prune[str(directory)] = now
    return path


def read_storage_label(name, status):
    if not retain_read(name):
        return "CURRENT_MEMORY_ONLY"
    if transient_financial_read(name, status):
        return "CURRENT_FINANCIAL_CACHE_10_MINUTES"
    return "RETAINED"


def bounded_storage():
    return os.environ.get("SIGBOT_STORAGE_MODE") == "TRADES_AND_CURRENT"


def retain_read(name):
    return not bounded_storage() or name not in MARKET_READS


def prune_market_audits(session, now):
    """Keep the complete two-hour model window plus ten minutes of margin."""
    from sqlalchemy import delete, text

    from sigbot.storage import Audit

    if not bounded_storage():
        return
    if session.bind.dialect.name == "postgresql":
        session.execute(text("SET LOCAL sigbot.market_retention = 'on'"))
    session.execute(delete(Audit).where(
        Audit.kind.in_(MARKET_AUDITS), Audit.at < now - timedelta(minutes=130),
    ))
