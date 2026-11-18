from __future__ import annotations

import difflib
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable
from uuid import uuid4

from sqlalchemy import select

from sigbot.domain import content_hash
from sigbot.errors import Conflict, SchemaMismatch, TransportUnknown
from sigbot.storage import (
    CatalogRun,
    EventGroup,
    Instrument,
    Market,
    MarketEventLink,
    Relation,
    Review,
    RuleVersion,
    Store,
)

CATEGORIES = {"议院控制权", "参议员选举", "众议员选区选举", "州长选举"}
STATUSES = {"OPEN", "CLOSED", "RESOLVED", "CANCELLED"}


@dataclass
class Page:
    markets: list[dict]
    next_cursor: str | None
    total: int | None = None


class Catalog:
    def __init__(self, store: Store):
        self.store = store

    def import_seed(self, path: str) -> dict:
        source = json.loads(Path(path).read_text())
        with self.store.transaction() as s:
            for item in source["markets"]:
                page_id = str(item["public_page_market_id"])
                key = "seed:" + page_id
                if s.get(Market, key):
                    continue
                s.add(
                    Market(
                        id=key,
                        tournament="PRIOR_RESEARCH_UNVERIFIED",
                        page_id=page_id,
                        api_id=None,
                        title=item["title"],
                        category=item["analytical_category_zh"],
                        source="PRIOR_RESEARCH_SEED",
                        rules_hash=None,
                        raw=item,
                    )
                )
                title_group = item.get("title_event_group")
                if title_group:
                    group_id = "seed-title:" + content_hash(title_group)
                    if s.get(EventGroup, group_id) is None:
                        s.add(
                            EventGroup(
                                id=group_id,
                                fields={"title_candidate": title_group, "official_event_id": None},
                                evidence="PRIOR_RESEARCH_TITLE_ONLY",
                            )
                        )
                        s.flush()
                    s.add(
                        MarketEventLink(
                            market_id=key,
                            event_id=group_id,
                            evidence={"status": "CANDIDATE_ONLY"},
                            verified=False,
                        )
                    )
        return {
            "status": "PARTIAL_SEED_ONLY",
            "source_records": len(source["markets"]),
            "tradable": 0,
            "api_ids_verified": 0,
        }

    def sync(
        self, tournament: str, fetch: Callable[[str | None], Page], all_statuses: bool
    ) -> dict:
        if not all_statuses:
            raise SchemaMismatch("ALL_STATUSES_REQUIRED")
        run_id = str(uuid4())
        evidence: dict = {
            "pages": [],
            "errors": [],
            "seen": [],
            "snapshot_consistency": "NON_ATOMIC",
        }
        with self.store.transaction() as s:
            s.add(CatalogRun(id=run_id, tournament=tournament, status="RUNNING", evidence=evidence))
        cursor = None
        cursors: set[str | None] = set()
        seen: set[str] = set()
        status = "PARTIAL"
        try:
            while True:
                if cursor in cursors:
                    raise SchemaMismatch("CURSOR_LOOP")
                cursors.add(cursor)
                page = fetch(cursor)
                evidence["pages"].append(
                    {
                        "cursor": cursor,
                        "hash": content_hash(page.markets),
                        "next": page.next_cursor,
                        "total": page.total,
                    }
                )
                if not page.markets and page.next_cursor is not None:
                    raise SchemaMismatch("EMPTY_PAGE_WITH_NEXT")
                with self.store.transaction() as s:
                    for item in page.markets:
                        if item.get("tournament_id") != tournament:
                            evidence["errors"].append("SCOPE_MISMATCH")
                            continue
                        mid = item.get("id")
                        if not isinstance(mid, str) or not mid:
                            raise SchemaMismatch("INVALID_EXTERNAL_ID")
                        if mid in seen:
                            evidence["errors"].append("DUPLICATE_ID:" + mid)
                            continue
                        seen.add(mid)
                        if item.get("status") not in STATUSES:
                            evidence["errors"].append("UNKNOWN_MARKET_STATUS:" + mid)
                        extra = sorted(
                            set(item) - {"id", "title", "tournament_id", "status", "category"}
                        )
                        if extra:
                            evidence.setdefault("extra_fields", []).extend(extra)
                        key = tournament + ":" + mid
                        m = s.get(Market, key)
                        if m:
                            m.raw = item
                            m.title = item["title"]
                        else:
                            s.add(
                                Market(
                                    id=key,
                                    tournament=tournament,
                                    api_id=mid,
                                    page_id=None,
                                    title=item["title"],
                                    category=item.get("category", "UNKNOWN"),
                                    source="SYNTHETIC_API"
                                    if tournament.startswith("SIM-")
                                    else "OFFICIAL_API",
                                    raw=item,
                                    rules_hash=None,
                                )
                            )
                if page.next_cursor is None:
                    if page.total is not None and page.total != len(seen):
                        evidence["errors"].append("TOTAL_MISMATCH")
                    status = "COMPLETE" if not evidence["errors"] else "PARTIAL"
                    break
                cursor = page.next_cursor
        except (SchemaMismatch, TransportUnknown) as exc:
            evidence["errors"].append(exc.code)
            status = "PARTIAL" if seen else "FAILED"
        evidence["seen"] = sorted(seen)
        with self.store.transaction() as s:
            run = s.get(CatalogRun, run_id)
            assert run is not None
            run.status, run.evidence = status, evidence
        return {"run_id": run_id, "status": status, "unique_markets": len(seen), **evidence}

    def rules(self, market_id: str, text: str, evidence: str, at: datetime) -> dict:
        digest = content_hash(text)
        with self.store.transaction() as s:
            m = s.scalar(select(Market).where(Market.id == market_id).with_for_update())
            if m is None:
                raise Conflict("MARKET_UNKNOWN")
            old = s.scalar(
                select(RuleVersion).where(
                    RuleVersion.market_id == market_id, RuleVersion.rules_hash == m.rules_hash
                )
            )
            change = m.rules_hash != digest
            if not s.scalar(
                select(RuleVersion).where(
                    RuleVersion.market_id == market_id, RuleVersion.rules_hash == digest
                )
            ):
                s.add(
                    RuleVersion(
                        market_id=market_id,
                        rules_hash=digest,
                        text=text,
                        evidence=evidence,
                        observed_at=at,
                    )
                )
            if change:
                m.rules_hash = digest
                # The catalog records observed rules; admitted instruments keep
                # their pinned version until an explicit compatible migration.
            diff = list(
                difflib.unified_diff((old.text if old else "").splitlines(), text.splitlines())
            )
        return {"changed": change, "rules_hash": digest, "diff": diff, "policy":"REPORT_ONLY",
                "old_rules_hash": old.rules_hash if old else None, "source": evidence, "observed_at":at.isoformat()}

    def review(
        self,
        instrument_id: str,
        rules_hash: str,
        mapping_hash: str,
        operator: str,
        expires_at: datetime,
    ) -> None:
        if not operator.strip():
            raise SchemaMismatch("OPERATOR_REQUIRED")
        with self.store.transaction() as s:
            i = s.scalar(select(Instrument).where(Instrument.id == instrument_id).with_for_update())
            if i is None or (i.rules_hash, i.mapping_hash) != (rules_hash, mapping_hash):
                raise Conflict("REVIEW_VERSION_MISMATCH")
            m = s.get(Market, i.market_id)
            if m is None or m.source == "PRIOR_RESEARCH_SEED":
                raise Conflict("SEED_NOT_REVIEWABLE_WITHOUT_OFFICIAL_MAPPING")
            s.add(
                Review(
                    instrument_id=i.id,
                    operator=operator,
                    rules_hash=rules_hash,
                    mapping_hash=mapping_hash,
                    expires_at=expires_at,
                )
            )
            i.reviewed, i.review_until = True, expires_at

    def group_candidate(self, market_id: str, fields: dict, evidence: str) -> str:
        if not evidence:
            raise SchemaMismatch("GROUP_EVIDENCE_REQUIRED")
        key = event_key(fields)
        with self.store.transaction() as s:
            if s.get(Market, market_id) is None:
                raise Conflict("MARKET_UNKNOWN")
            if s.get(EventGroup, key) is None:
                s.add(EventGroup(id=key, fields=fields, evidence=evidence))
                s.flush()
            if s.get(MarketEventLink, (market_id, key)) is None:
                s.add(
                    MarketEventLink(
                        market_id=market_id,
                        event_id=key,
                        evidence={"source": evidence},
                        verified=False,
                    )
                )
        return key

    def relation_candidate(self, left: str, right: str, evidence: dict) -> str:
        key = content_hash([left, right, evidence])
        with self.store.transaction() as s:
            if not s.get(Relation, key):
                s.add(
                    Relation(
                        id=key,
                        kind="SAME_EVENT_CANDIDATE_PAIR",
                        evidence={"left": left, "right": right, **evidence},
                        verified=False,
                    )
                )
        return key

    def audit(self, output: str) -> dict:
        with self.store.transaction() as s:
            markets = list(s.scalars(select(Market)))
            instruments = list(s.scalars(select(Instrument)))
            report = {
                "markets": len(markets),
                "instruments": len(instruments),
                "seed_records": sum(m.source == "PRIOR_RESEARCH_SEED" for m in markets),
                "rules_missing": sum(m.rules_hash is None for m in markets),
                "unmapped": sum(not any(i.market_id == m.id for i in instruments) for m in markets),
                "reviewed": sum(i.reviewed for i in instruments),
                "full_official_coverage": False,
                "candidate_event_groups": len(list(s.scalars(select(EventGroup)))),
                "categories": {
                    c: sum(m.category == c for m in markets)
                    for c in sorted({m.category for m in markets})
                },
            }
            folder = Path(output)
            folder.mkdir(parents=True, exist_ok=True)
            (folder / "markets.jsonl").write_text(
                "".join(json.dumps(m.raw, ensure_ascii=False) + "\n" for m in markets)
            )
            (folder / "data_quality.json").write_text(
                json.dumps(report, indent=2, ensure_ascii=False)
            )
            (folder / "relations.jsonl").write_text(
                "".join(
                    json.dumps({"kind": r.kind, "verified": r.verified, "evidence": r.evidence})
                    + "\n"
                    for r in s.scalars(select(Relation))
                )
            )
        return report


def event_key(fields: dict) -> str:
    required = ["year", "office", "jurisdiction", "district_or_seat", "election_kind"]
    if any(fields.get(k) is None for k in required):
        raise SchemaMismatch("EVENT_FIELDS_UNKNOWN")
    return content_hash({k: fields[k] for k in required})


def exhaustive_complements(left: dict, right: dict, rules_hashes: list[str], proof: str) -> bool:
    if not proof or not rules_hashes or set(left) != set(right) or not left:
        return False
    # N/A refund is not a binary payout and requires a separate proof.
    return all(left[k] in (0, 1) and right[k] in (0, 1) and left[k] + right[k] == 1 for k in left)
