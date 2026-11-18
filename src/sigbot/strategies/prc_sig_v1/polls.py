"""Original release deduplication and question-specific event series."""

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from sigbot.errors import Conflict
from sigbot.storage import PollObservationRow, PollReportRow, Store
from sigbot.strategies.prc_sig_v1.models import PollReport


class PollRepository:
    def __init__(self, store: Store):
        self.store = store

    def ingest_locked(self, session, report: PollReport) -> str:
        namespace = "SIM" if report.synthetic else "REAL"
        receipt = {
            "source_url": report.original.source_url,
            "received_at": report.original.received_at.isoformat(),
            "available_at": report.original.available_at.isoformat(),
            "source_sha256": report.original.sha256,
        }
        old = session.scalar(
            select(PollReportRow)
            .where(
                PollReportRow.namespace == namespace, PollReportRow.release_id == report.release_id
            )
            .with_for_update()
        )
        if old:
            if old.digest == report.digest:
                receipts = old.data.get("receipts", [])
                if receipt not in receipts:
                    old.data = {**old.data, "receipts": [*receipts, receipt]}
                return "DUPLICATE_OR_MIRROR"
            if old.first_public_at != report.original.published_at:
                raise Conflict("PRC_FIRST_PUBLIC_TIME_CANNOT_BE_RESET")
            old.data = {
                **old.data,
                "invalidated": True,
                "revisions": [*old.data.get("revisions", []), report.model_dump(mode="json")],
            }
            return "REVISED_OR_WITHDRAWN_SIGNAL_INVALIDATED"
        session.add(
            PollReportRow(
                namespace=namespace,
                release_id=report.release_id,
                digest=report.digest,
                first_public_at=report.original.published_at,
                received_at=report.original.received_at,
                available_at=report.original.available_at,
                data={
                    "original": report.model_dump(mode="json"),
                    "invalidated": report.withdrawn,
                    "receipts": [receipt],
                },
            )
        )
        for question in report.questions:
            session.add(
                PollObservationRow(
                    namespace=namespace,
                    release_id=report.release_id,
                    question_id=question.question_id,
                    event_key=question.event.event_key(report.synthetic),
                    series_key=question.series_key(report.organization, report.synthetic),
                    data=question.model_dump(mode="json"),
                )
            )
        session.flush()
        return "ORIGINAL_IMPORTED"

    def ingest(self, report: PollReport) -> str:
        try:
            with self.store.transaction() as session:
                return self.ingest_locked(session, report)
        except IntegrityError:
            # A concurrent first import may have won the composite unique key.
            # Re-read once; unrelated integrity failures still fail on this attempt.
            with self.store.transaction() as session:
                return self.ingest_locked(session, report)

    def series(self, event_key: str, synthetic: bool = True) -> list[dict]:
        with self.store.transaction() as session:
            return [
                row.data
                for row in session.scalars(
                    select(PollObservationRow).where(
                        PollObservationRow.namespace == ("SIM" if synthetic else "REAL"),
                        PollObservationRow.event_key == event_key,
                    )
                )
            ]
