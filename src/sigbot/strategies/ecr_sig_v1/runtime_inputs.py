"""Current collector data and operator reviews -> typed ECR execution snapshots.

Review signatures, fees and historical quotes are never manufactured here.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import select

from sigbot.domain import content_hash
from sigbot.errors import AuthorizationRequired, SchemaMismatch
from sigbot.storage import Position, StrategyEvent
from sigbot.strategies.ecr_sig_v1 import core
from sigbot.strategies.ecr_sig_v1.correspondence import correspondences
from sigbot.strategies.ecr_sig_v1.inputs import BookInput, MinuteInput, RuntimeReviews, Snapshot
from sigbot.strategies.ecr_sig_v1.liveinputs import MinuteSeries, fresh


class ReviewedFeed:
    def __init__(self, template: Snapshot | RuntimeReviews):
        if template.synthetic:
            raise AuthorizationRequired("REAL_REVIEW_INPUT_REQUIRED")
        self.template = (
            RuntimeReviews.from_snapshot(template) if isinstance(template, Snapshot) else template
        )
        self.series: dict[str, MinuteSeries] = {}
        self.rejected: dict[str, str] = {}
        self.books: dict = {}
        self.current_bindings: set[str] = set()
        aliases: dict = {}
        for raw in self.template.candidates:
            try:
                document = json.loads(raw.review.rules.text)
                if not isinstance(document, dict) or not isinstance(document.get("root"), dict):
                    continue
                details = document["root"].get("contract_details", {})
                if not isinstance(details, dict):
                    continue
            except ValueError:
                continue  # The per-frame evidence check emits an explicit rejection.
            key = (details.get("raceId"), details.get("stageId"))
            if not all(key):
                continue
            if any(not isinstance(value, (str, int)) or isinstance(value, bool) for value in key):
                raise SchemaMismatch("ECR_RUNTIME_INVALID_SIG_EVENT_IDENTIFIER")
            if key in aliases and aliases[key] != raw.event:
                raise SchemaMismatch("ECR_RUNTIME_SAME_SIG_EVENT_HAS_DIFFERENT_IDENTITIES")
            aliases[key] = raw.event

    @staticmethod
    def book(raw):
        return BookInput.model_validate(
            {
                "outcome_side": "YES",
                "bids": raw["bids"],
                "asks": raw["asks"],
                "source_at": raw.get("source_at"),
                "received_at": raw["received_at"],
                "verified_at": raw["received_at"],
                "version": content_hash(raw.get("source_version") or raw.get("hash") or raw),
                "gap_free": True,
            }
        )

    @staticmethod
    def minute(item):
        quote = item["quote"]
        # This is the receipt of a completed minute summary, not a newly
        # invented quote timestamp. Individual source receipts remain archived.
        available = datetime.fromisoformat(item["available_at"])
        return MinuteInput.model_validate(
            {
                "at": datetime.fromtimestamp(quote.at, timezone.utc),
                "received_at": available,
                "available_at": available,
                "sig_bid": quote.sig_bid,
                "sig_ask": quote.sig_ask,
                "external": [{"operator": v, "bid": b, "ask": a} for v, b, a in quote.external],
                "gap_free": item["strategy_complete"],
            }
        )

    def snapshot(self, observer, now):
        from sigbot.strategies.ecr_sig_v1.observer import sig_book

        if (observer.tid, observer.reads.reader.config.expected_account_id) != (
            self.template.tournament_id,
            self.template.account_id,
        ):
            raise AuthorizationRequired("ECR_RUNTIME_OBSERVER_SCOPE_MISMATCH")
        if observer.collector is None:
            raise SchemaMismatch("ECR_RUNTIME_COLLECTOR_REQUIRED")
        local, external, _ = observer.collector.snapshot()
        self.books, self.rejected = external, {}
        self.current_bindings = set()
        rows = {r["api_market_id"]: r for r in observer.rows}
        metadata = {(c["operator"], c["contract_id"]): c for c in observer.external}
        checks = (
            {
                (p["api_market_id"], p["operator"], p["external_contract_id"]): p
                for p in correspondences(
                    observer.rows, observer.nodes, observer.external, observer.tid
                )
            }
            if self.template.candidates
            else {}
        )
        candidates = []
        for raw in self.template.candidates:
            try:
                if raw.side != "YES":
                    raise SchemaMismatch("ECR_RUNTIME_NO_SIDE_FEED_NOT_IMPLEMENTED")
                row = rows.get(raw.market_id)
                nodes = observer.nodes.get(raw.market_id)
                if row is None or row["exchange_ids"] != [raw.exchange_id] or nodes is None:
                    raise SchemaMismatch("ECR_RUNTIME_CURRENT_MARKET_OR_RULES_MISSING")
                if content_hash(nodes) != content_hash(
                    json.loads(raw.review.rules.text, parse_float=Decimal)
                ):
                    raise SchemaMismatch("ECR_RUNTIME_SIG_RULES_CHANGED")
                from sigbot.strategies.ecr_sig_v1.correspondence import sig_predicate
                from sigbot.strategies.ecr_sig_v1.publicdata import party

                predicate, _ = sig_predicate(row, nodes, observer.tid)
                if predicate is None or (
                    predicate["year"],
                    predicate["state"],
                    predicate["office"],
                    predicate["district"],
                    predicate["election_kind"],
                    predicate["party"],
                ) != (
                    raw.event.year,
                    raw.event.state,
                    raw.event.office,
                    raw.event.district,
                    raw.event.election_kind,
                    party(raw.review.payoff_definition.predicate_subject),
                ):
                    raise SchemaMismatch("ECR_RUNTIME_REVIEW_EVENT_MISMATCH")
                books = []
                for mapping in raw.review.mappings:
                    key = (mapping.operator, mapping.external_contract_id)
                    current = metadata.get(key)
                    if current is None:
                        raise SchemaMismatch("ECR_RUNTIME_CURRENT_EXTERNAL_METADATA_MISSING")
                    pair = checks.get((raw.market_id, *key))
                    if not pair or not pair["unique_operator_match"]:
                        raise SchemaMismatch("ECR_RUNTIME_EXTERNAL_MAPPING_UNVERIFIED")
                    actual_rules = "\n".join(
                        str(current[k])
                        for k in ("description", "rules_primary", "rules_secondary")
                        if current.get(k)
                    )
                    if mapping.rules.text != actual_rules:
                        raise SchemaMismatch("ECR_RUNTIME_EXTERNAL_RULES_CHANGED")
                    book = external.get(key)
                    if book is None:
                        raise SchemaMismatch("ECR_RUNTIME_CURRENT_EXTERNAL_BOOK_MISSING")
                    books.append(book)
                self.current_bindings.add(raw.key)
                data = local.get(raw.exchange_id)
                if data is None:
                    raise SchemaMismatch("ECR_RUNTIME_CURRENT_SIG_BOOK_MISSING")
                received = datetime.fromisoformat(
                    data.get("validation_received_at") or data["received_at"]
                )
                started = datetime.fromisoformat(
                    data.get("validation_requested_at") or data["received_at"]
                )
                sig = sig_book(data, row, received, started)
                observation = {
                    "at": now.timestamp(),
                    "sig": sig,
                    "external": books,
                    "strategy_ready": bool(books) and all(fresh(b, now) for b in [sig, *books]),
                }
                series = self.series.setdefault(raw.key, MinuteSeries())
                series.add(
                    observation,
                    (
                        raw.rules_current_hash,
                        raw.review.mapping_hash,
                        tuple(
                            checks[(raw.market_id, m.operator, m.external_contract_id)][
                                "external_rules_hash"
                            ]
                            for m in raw.review.mappings
                        ),
                    ),
                )
                base, history, _ = series.window(now)
                if base is None or not history or not observation["strategy_ready"]:
                    raise SchemaMismatch("ECR_RUNTIME_FRESH_CONTIGUOUS_WINDOW_NOT_READY")
                by_at = {m["quote"].at: m for m in series.minutes if m["strategy_complete"]}
                updated = raw.model_dump(mode="json")
                updated.update(
                    direction_bucket=predicate["party"],
                    sig_book=self.book(sig).model_dump(mode="json"),
                    external_books=[
                        {
                            "operator": b["operator"],
                            "contract_id": b["contract_id"],
                            "book": self.book(b).model_dump(mode="json"),
                        }
                        for b in books
                    ],
                    baseline=self.minute(by_at[base.at]).model_dump(mode="json"),
                    history=[self.minute(by_at[h.at]).model_dump(mode="json") for h in history],
                    tradable_until=row["settlement_date"],
                )
                from sigbot.strategies.ecr_sig_v1.inputs import CandidateInput

                candidates.append(CandidateInput.model_validate(updated))
            except (SchemaMismatch, ValueError, KeyError) as exc:
                self.rejected[raw.key] = (
                    exc.code
                    if isinstance(exc, SchemaMismatch)
                    else "ECR_RUNTIME_REVIEW_OR_DATA_INVALID"
                )
        return Snapshot(
            synthetic=False,
            account_id=self.template.account_id,
            tournament_id=self.template.tournament_id,
            now=now,
            account=self.template.account.model_copy(deep=True),
            candidates=candidates,
        )

    def references(self, store, account_id, now):
        result = {}
        books = self.books
        templates = {c.event.event_key(False): c for c in self.template.candidates}
        with store.transaction() as s:
            for event in s.scalars(
                select(StrategyEvent).where(
                    StrategyEvent.account_id == account_id,
                    StrategyEvent.strategy_id == "ecr_sig",
                    StrategyEvent.strategy_version == "1.0",
                    StrategyEvent.state != "CLOSED",
                )
            ):
                raw = templates.get(event.event_key)
                position = s.get(Position, (account_id, event.instrument_id))
                if (
                    raw is None
                    or raw.key not in self.current_bindings
                    or position is None
                    or position.quantity <= 0
                ):
                    continue
                references: list[Decimal] = []
                for mapping in raw.review.mappings:
                    b = books.get((mapping.operator, mapping.external_contract_id))
                    if b is None or not fresh(b, now):
                        references = []
                        break
                    try:
                        references.append(
                            core.consume(
                                tuple(
                                    core.Level(Decimal(x["price"]), Decimal(x["size"]))
                                    for x in b["bids"]
                                ),
                                position.quantity,
                                False,
                            )[0]
                        )
                    except core.Reject:
                        references = []
                        break
                if references:
                    result[event.event_key] = min(references)
        return result
