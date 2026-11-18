"""Official all-status catalog receipts; title candidates never approve trading."""

from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import httpx

from sigbot.domain import content_hash
from sigbot.errors import AuthorizationRequired, SchemaMismatch, SigbotError
from sigbot.readcheck import ReadCheck, load_key, private_write
from sigbot.storage import CatalogRun, EventGroup, Market, MarketEventLink, Store

# Geographic dictionary, not a market whitelist; unknown/new titles remain visible.
STATES = dict(
    zip(
        "AL AK AZ AR CA CO CT DE FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE NV NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY DC".split(),
        [
            "Alabama",
            "Alaska",
            "Arizona",
            "Arkansas",
            "California",
            "Colorado",
            "Connecticut",
            "Delaware",
            "Florida",
            "Georgia",
            "Hawaii",
            "Idaho",
            "Illinois",
            "Indiana",
            "Iowa",
            "Kansas",
            "Kentucky",
            "Louisiana",
            "Maine",
            "Maryland",
            "Massachusetts",
            "Michigan",
            "Minnesota",
            "Mississippi",
            "Missouri",
            "Montana",
            "Nebraska",
            "Nevada",
            "New Hampshire",
            "New Jersey",
            "New Mexico",
            "New York",
            "North Carolina",
            "North Dakota",
            "Ohio",
            "Oklahoma",
            "Oregon",
            "Pennsylvania",
            "Rhode Island",
            "South Carolina",
            "South Dakota",
            "Tennessee",
            "Texas",
            "Utah",
            "Vermont",
            "Virginia",
            "Washington",
            "West Virginia",
            "Wisconsin",
            "Wyoming",
            "District of Columbia",
        ],
        strict=True,
    )
)


def title_event(title: str) -> dict:
    lower = title.casefold()
    if re.search(r"\b(control|majority)\b", lower) or re.search(
        r"\bU\.S\.\s+(House|Senate)\b", title, re.I
    ):
        return {"office": "CHAMBER_CONTROL", "signal_policy": "OBSERVE_ONLY", "verified": False}
    district = re.search(r"\b([A-Z]{2})-(\d{1,2}|AL)\b", title, re.I)
    states = [
        abbr
        for abbr, name in STATES.items()
        if re.search(r"\b" + re.escape(name) + r"\b", title, re.I)
    ]
    # Prefer the longest geographic name (West Virginia is not Virginia).
    if states:
        length = max(len(STATES[s]) for s in states)
        states = [s for s in states if len(STATES[s]) == length]
    state = (
        district[1].upper()
        if district and district[1].upper() in STATES
        else states[0]
        if len(states) == 1
        else None
    )
    office = (
        "HOUSE"
        if district and "house" in lower
        else "SENATE"
        if "senate" in lower
        else "GOVERNOR"
        if "governor" in lower
        else "UNKNOWN"
    )
    if state is None or office == "UNKNOWN":
        return {"office": "UNKNOWN", "signal_policy": "REJECT_UNMAPPED", "verified": False}
    fields = {
        "year": 2026,
        "state": state,
        "office": office,
        "district": district[2].upper().zfill(2) if office == "HOUSE" and district else "STATEWIDE",
        "election_kind": "UNVERIFIED",
        "seat": "UNVERIFIED",
        "boundary_version": "UNVERIFIED",
    }
    # One provisional group per underlying geography/office, irrespective of party/card.
    return {
        **fields,
        "candidate_group": "CANDIDATE-"
        + content_hash([2026, state, office, fields["district"]])[:24],
        "signal_policy": "REQUIRES_EVENT_RULE_REVIEW",
        "verified": False,
        "gaps": [
            "official question/seat/election kind",
            "district boundary version",
            "binary outcome semantics",
        ],
    }


def catalog_rows(markets: list[dict], tournament_id: str) -> list[dict]:
    result = []
    ids = set()
    exchanges: set[str] = set()
    for market in markets:
        mid = market["id"]
        if mid in ids:
            raise SchemaMismatch("PRC_DUPLICATE_API_MARKET_ID")
        ids.add(mid)
        contexts = [
            c for c in market["contexts"] if c.get("tournament", {}).get("id") == tournament_id
        ]
        if len(contexts) != 1:
            raise SchemaMismatch("PRC_MARKET_CONTEXT_AMBIGUOUS")
        context = contexts[0]
        official_exchanges = {e["id"]: e for e in market["exchanges"]}
        context_ids = {e["id"] for e in context["exchanges"]}
        if context_ids != set(official_exchanges):
            raise SchemaMismatch("PRC_CONTEXT_EXCHANGE_SET_MISMATCH")
        if exchanges & context_ids:
            raise SchemaMismatch("PRC_EXCHANGE_REUSED_ACROSS_MARKETS")
        exchanges.update(context_ids)
        if context["status"] not in {"open", "closed", "settled"}:
            raise SchemaMismatch("PRC_CONTEXT_STATUS_UNRECOGNIZED")
        result.append(
            {
                "api_market_id": mid,
                "page_id": None,
                "page_id_verification": "NOT_VERIFIED",
                "exchange_ids": sorted(context_ids),
                "exchange_options": {k: v["option"] for k, v in official_exchanges.items()},
                "api_exchange_link_verified": True,
                "binary_sides_verified": False,
                "event_mapping_verified": False,
                "title": market["title"],
                "status": context["status"],
                "global_status": market["status"],
                "settlement_date": market["settlementDate"],
                "is_composite": market["isComposite"],
                "is_multi_outcome": market["isMultiOutcome"],
                "event_candidate": title_event(market["title"]),
                "rule_review": "UNAPPROVED",
                "tradable": False,
                "raw_hash": content_hash(market),
            }
        )
    return result


def persist(store, run_id, tournament_id, markets, rows, report):
    with store.transaction() as s:
        for market, row in zip(markets, rows, strict=True):
            key = tournament_id + ":" + row["api_market_id"]
            old = s.get(Market, key)
            if old:
                old.raw = market
                old.title = market["title"]
            else:
                s.add(
                    Market(
                        id=key,
                        tournament=tournament_id,
                        page_id=None,
                        api_id=market["id"],
                        title=market["title"],
                        category=row["event_candidate"]["office"],
                        source="SYNTHETIC_API" if report.get("layer") == "MOCK" else "OFFICIAL_API",
                        rules_hash=None,
                        raw=market,
                    )
                )
            group = row["event_candidate"].get("candidate_group")
            if group:
                if s.get(EventGroup, group) is None:
                    s.add(
                        EventGroup(
                            id=group,
                            fields=row["event_candidate"],
                            evidence="OFFICIAL_TITLE_ONLY_NOT_ADMISSION",
                        )
                    )
                    s.flush()
                if s.get(MarketEventLink, (key, group)) is None:
                    s.add(
                        MarketEventLink(
                            market_id=key,
                            event_id=group,
                            evidence={
                                "run_id": run_id,
                                "source": "OFFICIAL_API_TITLE",
                                "verified_fields": ["api_market_id", "exchange_ids"],
                            },
                            verified=False,
                        )
                    )
        s.add(CatalogRun(id=run_id, tournament=tournament_id, status="COMPLETE", evidence=report))


def refresh_reference(
    directory: Path, *, pinned_spec_path="evidence/api/openapi-2026-10-03.json"
) -> dict:
    """Observe public documentation; differences never revoke existing adapters.

    User policy 2026-10-18: REPORT_ONLY. Actual authenticated response schemas,
    identity and accounting checks remain in their existing consumers.
    """
    url = "https://sig.thesuper.market/api/v1/docs"
    directory.mkdir(parents=True, exist_ok=True)
    result = {
        "url": url,
        "policy": "REPORT_ONLY",
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "spec_semantically_matches_pinned": None,
        "public_get_requests": 1,
    }
    try:
        response = httpx.get(url, timeout=30, follow_redirects=False, trust_env=False)
        response.raise_for_status()
        text = response.text
        start = text.index("Scalar.createApiReference")
        payload, _ = json.JSONDecoder().raw_decode(text[text.index("{", start) :])
        spec = payload["content"]
        if not isinstance(spec, dict):
            raise ValueError("OpenAPI document is not an object")
    except (httpx.HTTPError, ValueError, KeyError) as exc:
        result.update(status="REFERENCE_UNAVAILABLE_REPORTED", error_type=type(exc).__name__)
    else:
        local = json.loads(Path(pinned_spec_path).read_text())
        digest = hashlib.sha256(response.content).hexdigest()
        archive = directory / "observations" / digest
        archive.mkdir(parents=True, exist_ok=True)
        (archive / "PUBLIC_REFERENCE.html").write_bytes(response.content)
        (archive / "OPENAPI.json").write_text(json.dumps(spec, indent=2) + "\n")
        (directory / "PUBLIC_REFERENCE.html").write_bytes(response.content)
        same = content_hash(local) == content_hash(spec)
        result.update(
            http_status=response.status_code,
            body_sha256=digest,
            spec_semantically_matches_pinned=same,
            pinned_digest=content_hash(local),
            observed_digest=content_hash(spec),
            evidence_path=str(archive),
            status="MATCHES_PINNED" if same else "DOCUMENT_CHANGE_REPORTED",
        )
    (directory / "REFERENCE_OBSERVATION.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def sync(args, contract_file, transport=None) -> dict:
    if not args.authorize_read:
        raise AuthorizationRequired("PRC_EXPLICIT_CATALOG_READ_REQUIRED")
    reference = (
        refresh_reference(Path("reports/strategies/prc_sig_v1"))
        if transport is None
        else {"mock": True, "public_get_requests": 0}
    )
    probe = ReadCheck(
        load_key(args.env_file, args.key_name),
        tournament_slug=args.tournament_slug,
        root=Path("runtime/readonly"),
        max_requests=args.max_requests,
        max_pages=args.max_pages,
        seconds=args.seconds,
        contract_file=contract_file,
        transport=transport,
    )
    report = {
        **probe.report,
        "strategy_id": "prc_sig",
        "strategy_version": "1.0",
        "public_reference": reference,
        "validation_scope": "ALL_STATUS_MARKET_CATALOG_ONLY",
        "catalog_complete": False,
        "markets": [],
        "actual_signal_candidates": 0,
        "real_poll_reports": 0,
        "approved_underlying_events": 0,
        "expected_pnl": None,
        "competition_performance": "NOT_VALIDATED",
    }
    try:
        account = probe.check("account_identity", lambda: probe.call("account_identity"))
        if not account["id"] or account["id"].startswith("SIM-"):
            raise AuthorizationRequired("PRC_REAL_ACCOUNT_ID_INVALID")
        probe.reader.config.expected_account_id = account["id"]
        report["account_id_sha256"] = hashlib.sha256(account["id"].encode()).hexdigest()
        tournaments, _ = probe.check(
            "tournament_list",
            lambda: probe.pages(
                "tournament_list", query={"status": "any", "limit": 100, "offset": 0}
            ),
        )
        matched = [t for t in tournaments if t["slug"] == args.tournament_slug]
        if len(matched) != 1:
            raise AuthorizationRequired("PRC_TOURNAMENT_NOT_UNIQUELY_ACCESSIBLE")
        tid = matched[0]["id"]
        probe.reader.config.expected_tournament_id = tid
        probe.reader.config.expected_tournament_slug = args.tournament_slug
        cup = probe.check(
            "tournament_identity",
            lambda: probe.call("tournament_identity", path_params={"slug": args.tournament_slug}),
        )
        if cup["isPendingEnrolment"]:
            raise AuthorizationRequired("PRC_TOURNAMENT_ENROLMENT_PENDING")
        markets, _ = probe.check(
            "market_list",
            lambda: probe.pages(
                "market_list", query={"tournamentId": tid, "status": "any", "limit": 100}
            ),
        )
        rows = catalog_rows(markets, tid)
        office = Counter(r["event_candidate"]["office"] for r in rows)
        groups = {r["event_candidate"].get("candidate_group") for r in rows} - {None}
        report.update(
            status="PASS_CATALOG_ONLY",
            catalog_complete=True,
            tournament_id=tid,
            tournament_status=cup["status"],
            snapshot_consistency="NON_ATOMIC_PAGINATED_OBSERVATION",
            markets=rows,
            funnel={
                "official_catalog_markets": len(rows),
                "statuses": dict(Counter(r["status"] for r in rows)),
                "title_candidate_cards_by_office": dict(office),
                "title_candidate_event_groups": len(groups),
                "rule_and_event_approved_cards": 0,
                "real_original_poll_reports": 0,
                "comparable_pairs": 0,
                "complete_15_minute_windows": 0,
                "actual_signal_candidates": 0,
                "allocated_real_plans": 0,
            },
            gaps=[
                "Title candidates lack official event/seat/boundary and binary outcome review.",
                "Web page IDs are not inferred from API IDs.",
                "No original poll ingestion source/reviewed contrast variances supplied.",
                "No historical complete minute windows captured.",
                "SIG fee evidence remains unverified.",
                "No consistent account snapshot acceptance; normal live OMS bridge disabled.",
            ],
        )
        report["catalog_persistence"] = "DURABLE_JSON_RECEIPTS"
        report["database_projection"] = "NOT_CONFIGURED"
        if getattr(args, "catalog_database_url", None):
            store = Store(args.catalog_database_url, namespace="readonly")
            try:
                persist(store, probe.run_id, tid, markets, rows, report)
            finally:
                store.engine.dispose()
            report["database_projection"] = "POSTGRES_PROJECTED"
    except SigbotError as exc:
        report.update(status="BLOCKED", stop_code=exc.code)
    finally:
        probe.reader.close()
        probe.key = ""
    report["read_requests"] = probe.budget.count
    report["market_page_receipts"] = page_receipts(probe.requests)
    report["finished_at"] = datetime.now(timezone.utc).isoformat()
    private_write(
        probe.private / "prc-catalog-summary.json",
        json.dumps(report, default=str, indent=2).encode(),
    )
    return report


def page_receipts(requests):
    pages = []
    for request in requests:
        if request["capability"] != "market_list" or not request.get("schema_validated"):
            continue
        page = request.get("market_page")
        raw = {"data": [None] * page["rows"], "pagination": page["pagination"]} if page else json.loads(Path(request["private_evidence_path"]).read_text())
        pg = raw["pagination"]
        pages.append(
            {
                "rows": len(raw["data"]),
                "total": pg.get("total"),
                "has_more": pg["hasMore"],
                "next_cursor_sha256": content_hash(pg["nextCursor"])
                if pg.get("nextCursor")
                else None,
                "response_sha256": request["response_sha256"],
                "received_at": request["received_at"],
            }
        )
    return pages
