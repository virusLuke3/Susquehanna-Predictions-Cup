"""Receipt-bound poll import and offline audits of the shared SIG collector."""

from __future__ import annotations

import hashlib
import json
from collections import Counter, defaultdict
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

from sigbot.domain import utc_time
from sigbot.errors import SchemaMismatch
from sigbot.storage import Store
from sigbot.strategies.prc_sig_v1.acquisition import ROOT, now, write_json
from sigbot.strategies.prc_sig_v1.models import PollReport
from sigbot.strategies.prc_sig_v1.polls import PollRepository

SHARED = Path("runtime/readonly/strategies/ecr_sig_v1")


def import_poll(input_path: Path, root: Path = ROOT, store: Store | None = None) -> dict:
    """Import explicitly supplied questions, never infer reviewed event identities.

    The application binds the actual archived bytes, text, and receipt times.
    Human input owns original report identity, survey design and review flags.
    No database is opened/migrated implicitly; REAL rows require PostgreSQL.
    """
    report = PollReport.model_validate_json(input_path.read_text())
    if report.synthetic or report.original.kind != "ORIGINAL_PUBLIC":
        raise SchemaMismatch("PRC_REAL_POLL_IMPORT_REQUIRES_ORIGINAL_PUBLIC_NAMESPACE")
    if not report.questions:
        raise SchemaMismatch("PRC_POLL_IMPORT_REQUIRES_EVENT_QUESTIONS")
    for q in report.questions:
        if any(
            str(v).startswith("SIM-") for v in q.event.model_dump().values()
        ) or q.question_id.startswith("SIM-"):
            raise SchemaMismatch("PRC_SYNTHETIC_QUESTION_IN_REAL_IMPORT")
    index = root / "index.json"
    documents = json.loads(index.read_text())["documents"] if index.exists() else []
    match = None
    for document in documents:
        versions = [document, *document.get("prior_versions", [])]
        for version in versions:
            evidence = [
                {
                    **version["receipt"],
                    "url": version["source_url"],
                    "text_path": version["text_path"],
                    "text_sha256": version["text_sha256"],
                    "received_at": version["receipt"]["received_at"],
                    "available_at": version["available_at"],
                }
            ]
            evidence.extend(
                {**p, "available_at": version["available_at"]}
                for p in version["pdf_documents"]
                if p.get("text_path")
            )
            for item in evidence:
                if (
                    item["url"] == report.original.source_url
                    and item["text_sha256"] == report.original.sha256
                    and utc_time(item["received_at"]) == report.original.received_at
                ):
                    match = item
    if match is None:
        raise SchemaMismatch("PRC_POLL_ORIGINAL_NOT_IN_ACQUISITION_ARCHIVE")
    text_path, raw_path = Path(match["text_path"]), Path(match["raw_path"])
    if not all(p.resolve().is_relative_to(root.resolve()) for p in (text_path, raw_path)):
        raise SchemaMismatch("PRC_POLL_ARCHIVE_PATH_OUTSIDE_ROOT")
    if (
        match["http_status"] != 200
        or hashlib.sha256(raw_path.read_bytes()).hexdigest() != match["raw_sha256"]
    ):
        raise SchemaMismatch("PRC_POLL_ARCHIVED_RAW_MISMATCH")
    text = text_path.read_text()
    if (
        text != report.original.text
        or hashlib.sha256(text.encode()).hexdigest() != match["text_sha256"]
    ):
        raise SchemaMismatch("PRC_POLL_ARCHIVED_TEXT_MISMATCH")
    if report.original.received_at != utc_time(
        match["received_at"]
    ) or report.original.available_at < utc_time(match["available_at"]):
        raise SchemaMismatch("PRC_POLL_CANNOT_BACKDATE_RECEIPT_OR_AVAILABILITY")
    if report.original.available_at > now():
        raise SchemaMismatch("PRC_POLL_IMPORT_AVAILABILITY_IN_FUTURE")
    if store is not None and (
        store.engine.dialect.name != "postgresql" or store.namespace != "readonly"
    ):
        raise SchemaMismatch("PRC_REAL_POLL_DATABASE_REQUIRES_READONLY_POSTGRES")
    path = root / "normalized" / report.release_id / (report.digest + ".json")
    previous_reports = [
        PollReport.model_validate_json(p.read_text()) for p in path.parent.glob("*.json")
    ]
    if any(r.original.published_at != report.original.published_at for r in previous_reports):
        raise SchemaMismatch("PRC_FIRST_PUBLIC_TIME_CANNOT_BE_RESET")
    result = PollRepository(store).ingest(report) if store else "ARCHIVED_NOT_PROJECTED_TO_DATABASE"
    write_json(path, report.model_dump(mode="json"))
    published = report.original.published_at
    return {
        "layer": "LOCAL_DATA_IMPORT",
        "status": result,
        "release_id": report.release_id,
        "report_digest": report.digest,
        "questions": len(report.questions),
        "database_projection": "POSTGRES" if store else "NOT_CONFIGURED",
        "first_public_verified": report.first_public_verified,
        "timely_receipt": bool(
            published
            and report.first_public_verified
            and 0 <= (report.original.received_at - published).total_seconds() <= 60
        ),
        "network_calls": 0,
        "real_orders": 0,
        "authorized_for_live": False,
        "normalized_path": str(path.resolve()),
        "review_flags_source": "EXPLICIT_LOCAL_INPUT_NOT_ASSIGNED_BY_COLLECTOR",
        "prior_signal_invalidated": any(r.digest != report.digest for r in previous_reports),
    }


def records(path: Path, quality: Counter):
    """Read a fixed file prefix; a concurrent unfinished final line is PARTIAL."""
    if not path.exists():
        return
    size = path.stat().st_size
    with path.open("rb") as stream:
        while stream.tell() < size:
            raw = stream.readline(size - stream.tell())
            if not raw.endswith(b"\n"):
                quality["unfinished_final_records"] += 1
                break
            try:
                value = json.loads(raw)
                if not isinstance(value, dict):
                    quality["invalid_record_shape"] += 1
                    continue
                yield value
            except (ValueError, UnicodeError):
                quality["invalid_json_records"] += 1


def quote_coverage(path: Path, at, minutes: int = 45) -> dict:
    cutoff = at - timedelta(minutes=minutes)
    buckets: dict = defaultdict(set)
    quality: Counter = Counter()
    missing_clock, future, invalid, rows = 0, 0, 0, 0
    latest = None
    instruments = set()
    for packet in records(path, quality):
        quotes = packet.get("quotes", {})
        if not isinstance(quotes, dict):
            quality["invalid_quote_packet_shape"] += 1
            continue
        for exchange, quote in quotes.items():
            if not isinstance(quote, dict):
                invalid += 1
                continue
            try:
                received = utc_time(quote["received_at"])
                if received < cutoff:
                    continue
                if received > at:
                    future += 1
                    continue
                rows += 1
                if latest is None or received > latest:
                    latest = received
                instruments.add(exchange)
                if quote.get("source_at") is None:
                    missing_clock += 1
                bid_value, ask_value = quote.get("bestBid"), quote.get("bestAsk")
                if not isinstance(bid_value, str) or not isinstance(ask_value, str):
                    invalid += 1
                    continue
                bid, ask = Decimal(bid_value), Decimal(ask_value)
                if not bid.is_finite() or not ask.is_finite() or not 0 < bid <= ask < 1:
                    invalid += 1
                    continue
                bucket = int(received.timestamp() // 60) * 60
                if bucket + 60 <= at.timestamp():
                    buckets[(exchange, bucket)].add(received.timestamp())
            except (KeyError, ValueError, TypeError, ArithmeticError):
                invalid += 1
    summaries: dict = defaultdict(
        lambda: {
            "sampled_closed_minutes": 0,
            "sampling_complete_minutes": 0,
            "strategy_complete_minutes": 0,
        }
    )
    for (exchange, start), samples in buckets.items():
        times = [float(start), *sorted(samples), float(start + 60)]
        complete = (
            len(samples) >= 3 and max(b - a for a, b in zip(times, times[1:], strict=False)) <= 25
        )
        summaries[exchange]["sampled_closed_minutes"] += 1
        summaries[exchange]["sampling_complete_minutes"] += int(complete)
    return {
        "scope": f"LAST_{minutes}_MINUTES_REST_RECEIPT_SAMPLING_NOT_ALL_TICKS",
        "file_present": path.exists(),
        "recent_quote_rows": rows,
        "instruments_observed": len(instruments),
        "latest_received_at": latest.isoformat() if latest else None,
        "latest_age_seconds": (at - latest).total_seconds() if latest else None,
        "source_clock_missing_rows": missing_clock,
        "future_rows_rejected": future,
        "invalid_or_empty_quotes": invalid,
        "file_quality": dict(quality),
        "instruments_with_complete_sample_minutes": sum(
            v["sampling_complete_minutes"] > 0 for v in summaries.values()
        ),
        "sampling_complete_minutes": sum(
            v["sampling_complete_minutes"] for v in summaries.values()
        ),
        "strategy_complete_minutes": 0,
        "by_exchange": dict(summaries),
        "blocking_reasons": [
            "BULK_SOURCE_TIME_NOT_VERIFIED",
            "RULE_MAPPING_AND_WINDOW_EVIDENCE_UNAPPROVED",
        ],
    }


def status(root: Path = ROOT, shared: Path = SHARED, at=None) -> dict:
    at = at or now()
    index = root / "index.json"
    documents = json.loads(index.read_text())["documents"] if index.exists() else []
    imported = [
        PollReport.model_validate_json(p.read_text())
        for p in sorted((root / "normalized").glob("*/*.json"))
    ]
    releases = {r.release_id for r in imported}
    quality: Counter = Counter()
    depth_markets: set[str] = set()
    external: Counter = Counter()
    latest_book = None
    for packet in records(shared / "book-observations.jsonl", quality):
        for observation in packet.get("observations", []):
            book = observation.get("sig")
            if not book:
                continue
            received = utc_time(book["received_at"])
            if not at - timedelta(minutes=45) <= received <= at:
                continue
            latest_book = max(received, latest_book) if latest_book else received
            if book.get("bids") and book.get("asks"):
                depth_markets.add(observation["api_market_id"])
            external.update(b.get("operator", "UNKNOWN") for b in observation.get("external", []))
    observer_status = shared / "status.json"
    observer = json.loads(observer_status.read_text()) if observer_status.exists() else {}
    return {
        "layer": "LOCAL_DATA_AUDIT",
        "status": "PARTIAL_DATA_TRADE_GATES_BLOCKED",
        "observed_at": at.isoformat(),
        "network_calls": 0,
        "credentials_loaded": False,
        "real_orders": 0,
        "prc": {
            "archived_release_documents": len(documents),
            "original_report_identity_not_inferred_from_url": True,
            "archived_pdf_documents": sum(len(d["pdf_documents"]) for d in documents),
            "external_table_links_pending": sum(len(d["external_table_links"]) for d in documents),
            "normalized_releases": len(releases),
            "normalized_versions": len(imported),
            "normalized_event_questions": sum(len(r.questions) for r in imported),
            "normalized_reports_with_unknown_variance": sum(
                any(q.contrast_variance is None for q in r.questions) for r in imported
            ),
            "qualified_candidates": 0,
            "candidate_evaluation": "NOT_RUN_INPUT_GATES_INCOMPLETE",
        },
        "shared_sig_quotes": quote_coverage(shared / "quotes.jsonl", at),
        "shared_depth": {
            "recent_markets_with_two_sided_books": len(depth_markets),
            "latest_received_at": latest_book.isoformat() if latest_book else None,
            "file_quality": dict(quality),
        },
        "ecr": {
            "recent_external_book_observations": dict(external),
            "observer_updated_at": observer.get("updated_at"),
            "observer_signal_evaluations_reported": observer.get("signals_evaluated"),
            "observer_strategy_complete_minutes_reported": observer.get(
                "strategy_complete_minutes"
            ),
        },
        "remaining": [
            "QUESTION_EVENT_AND_PAYOFF_REVIEW",
            "PRC_COMPARABLE_PAIR_VARIANCE_AND_FIRST_PUBLIC_CLOCK",
            "PRC_60_SECOND_CAPTURE_AND_PRE_POST_WINDOW",
            "ECR_SOURCE_CLOCK_AND_CONTINUOUS_WINDOWS",
            "SIG_FEES_CONSISTENT_ACCOUNT_AND_LIVE_BRIDGE",
        ],
    }
