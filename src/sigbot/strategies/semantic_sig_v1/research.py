"""Bounded official reads and passive reuse of the existing SIG collector."""

from __future__ import annotations

import hashlib
import json
import os
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import httpx

from sigbot.domain import content_hash, utc_time
from sigbot.errors import AuthorizationRequired, ContractNotVerified, SchemaMismatch, SigbotError
from sigbot.official import OfficialSchema
from sigbot.readcheck import ReadCheck, load_key, private_write
from sigbot.shared_marketdata import SharedSigCollector
from sigbot.strategies.mm_sig_v1.data import participant_reference
from sigbot.strategies.semantic_sig_v1.core import number, scan, seat_coverage
from sigbot.strategies.semantic_sig_v1.registry import (
    groups,
    inputs,
    latest_rules,
    map_catalog,
    read_csv,
    record_rules,
    review_status,
    write_json,
)

REPORTS = Path("reports/strategies/semantic_sig_v1")
PRIVATE = Path("runtime/readonly/strategies/semantic_sig_v1")
CONTRACT = "contracts/strategies/semantic_sig_v1.reads.json"


class PassiveCollector(SharedSigCollector):
    def set_targets(self, contracts, *, market_ids=()):
        # Read the existing source without overwriting another strategy's priority request.
        if contracts:
            raise ValueError("SEMANTIC_PASSIVE_SOURCE_ONLY")
        self.focus = set(market_ids) & set(self.mapping)

    def close(self):
        pass


def reference_check(contract_file, folder, transport=None):
    contract = json.loads(Path(contract_file).read_text())
    schema = OfficialSchema(contract.get("schema_path", "evidence/api/openapi-2026-10-03.json"))
    operations = [
        (contract["capabilities"][name]["openapi_path"], "get")
        for name in [
            "account_identity",
            "tournament_list",
            "tournament_identity",
            "market_list",
            "market_detail",
        ]
    ]
    schema_hash = hashlib.sha256(Path(schema.path).read_bytes()).hexdigest()
    for cap in contract["capabilities"].values():
        if (
            cap["http_method"] != "GET"
            or not cap.get("evidence")
            or any(
                e.get("source") != "OFFICIAL"
                or e.get("evidence_path") != schema.path
                or e.get("sha256") != schema_hash
                for e in cap["evidence"]
            )
        ):
            raise ContractNotVerified("SEMANTIC_READ_EVIDENCE_BINDING_INVALID")
    url = "https://sig.thesuper.market/api/v1/docs"
    try:
        with httpx.Client(
            transport=transport, timeout=20, trust_env=False, follow_redirects=False
        ) as client:
            response = client.get(url)
            response.raise_for_status()
        text = response.text
        start = text.index("Scalar.createApiReference")
        payload, _ = json.JSONDecoder().raw_decode(text[text.index("{", start) :])
        spec = payload["content"]
    except (httpx.HTTPError, ValueError, KeyError) as exc:
        raise ContractNotVerified("SEMANTIC_CURRENT_OFFICIAL_REFERENCE_UNAVAILABLE") from exc
    folder = Path(folder)
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "PUBLIC_REFERENCE.html").write_bytes(response.content)
    write_json(folder / "openapi.json", spec)
    match = content_hash(participant_reference(spec, operations)) == content_hash(
        participant_reference(schema.spec, operations)
    )
    result = {
        "source_url": url,
        "received_at": datetime.now(timezone.utc).isoformat(),
        "schema_sha256": hashlib.sha256(Path(schema.path).read_bytes()).hexdigest(),
        "response_sha256": hashlib.sha256(response.content).hexdigest(),
        "reviewed_operations": operations,
        "matches_reviewed_operations": match,
        "rules_field_documented": any(
            k.lower() in {"rules", "info", "description"}
            for k in spec["components"]["schemas"]["Market"]["properties"]
        ),
        "real_writes": 0,
    }
    write_json(folder / "REFERENCE_CHECK.json", result)
    if not match:
        raise ContractNotVerified("SEMANTIC_OFFICIAL_CONTRACT_CHANGED_REVIEW_REQUIRED")
    return result


def collect(args, transport=None):
    if not args.authorize_read:
        raise AuthorizationRequired("SEMANTIC_EXPLICIT_READ_REQUIRED")
    report: dict = {
        "layer": "MOCK" if transport is not None else "REAL_READ",
        "catalog_complete": False,
        "real_writes": 0,
        "real_orders": 0,
        "real_cancels": 0,
        "requests": [],
    }
    probe = None
    try:
        report["reference"] = reference_check(
            args.contract, Path(args.output) / "reference", transport
        )
        # Only the authorized application loads the key. It never enters reports or exceptions.
        probe = ReadCheck(
            load_key(args.env_file, args.key_name),
            tournament_slug=args.tournament_slug,
            root=Path("runtime/readonly"),
            max_requests=args.max_requests,
            max_pages=args.max_pages,
            seconds=args.seconds,
            contract_file=args.contract,
            transport=transport,
        )
        account = probe.check("account_identity", lambda: probe.call("account_identity"))
        if not account["id"] or account["id"].startswith("SIM-") and transport is None:
            raise AuthorizationRequired("SEMANTIC_REAL_ACCOUNT_REQUIRED")
        probe.reader.config.expected_account_id = account["id"]
        report["account_id_sha256"] = hashlib.sha256(account["id"].encode()).hexdigest()
        cups, _ = probe.check(
            "tournament_list",
            lambda: probe.pages(
                "tournament_list", query={"status": "any", "limit": 100, "offset": 0}
            ),
        )
        found = [c for c in cups if c["slug"] == args.tournament_slug]
        if len(found) != 1:
            raise AuthorizationRequired("SEMANTIC_TOURNAMENT_SCOPE_AMBIGUOUS")
        tid = found[0]["id"]
        probe.reader.config.expected_tournament_id = tid
        probe.reader.config.expected_tournament_slug = args.tournament_slug
        cup = probe.check(
            "tournament_identity",
            lambda: probe.call("tournament_identity", path_params={"slug": args.tournament_slug}),
        )
        if cup["isPendingEnrolment"]:
            raise AuthorizationRequired("SEMANTIC_ENROLMENT_PENDING")
        raw, _ = probe.check(
            "market_list",
            lambda: probe.pages(
                "market_list", query={"tournamentId": tid, "status": "any", "limit": 100}
            ),
        )
        report.update(
            status="CATALOG_READ_COMPLETE",
            catalog_complete=True,
            tournament_id=tid,
            tournament_slug=args.tournament_slug,
            markets_observed=len(raw),
            schema_path=probe.reader.schema.path,
            schema_sha256=hashlib.sha256(Path(probe.reader.schema.path).read_bytes()).hexdigest(),
        )
        report["rules_status"] = "BLOCKED_NO_DOCUMENTED_INFO_RULES_CAPABILITY"
    except SigbotError as exc:
        report.update(status="BLOCKED", stop_code=exc.code)
    finally:
        if probe is not None:
            report["requests"] = probe.requests
            report["checks"] = probe.checks
            report["read_requests"] = probe.budget.count
            probe.reader.close()
            probe.key = ""
    write_json(Path(args.output) / "catalog_receipt_manifest.json", report)
    if report["catalog_complete"] and report["layer"] == "REAL_READ":
        args.catalog = str(Path(args.output) / "catalog_receipt_manifest.json")
        report["research"] = audit(args)
    return report, 0 if report["catalog_complete"] else 2


def captured_catalog(path):
    report = json.loads(Path(path).read_text())
    if report.get("layer") != "REAL_READ" or report.get("catalog_complete") is not True:
        raise SchemaMismatch("SEMANTIC_REAL_COMPLETE_CATALOG_REQUIRED")
    receipts = [r for r in report["requests"] if r["capability"] == "market_list"]
    if not receipts:
        raise SchemaMismatch("SEMANTIC_RAW_CATALOG_RECEIPTS_REQUIRED")
    raw, cursors = [], set()
    for index, receipt in enumerate(receipts):
        body = Path(receipt["private_evidence_path"]).read_bytes()
        if hashlib.sha256(body).hexdigest() != receipt["response_sha256"]:
            raise SchemaMismatch("SEMANTIC_CATALOG_RECEIPT_HASH_MISMATCH")
        utc_time(receipt["received_at"])
        if not receipt.get("schema_validated") or not receipt.get("scope_validated"):
            raise SchemaMismatch("SEMANTIC_CATALOG_RECEIPT_NOT_VALIDATED")
        data = json.loads(body)
        if report.get("schema_path"):
            schema_path = Path(report["schema_path"])
            if hashlib.sha256(schema_path.read_bytes()).hexdigest() != report.get("schema_sha256"):
                raise SchemaMismatch("SEMANTIC_CATALOG_SCHEMA_HASH_CHANGED")
            OfficialSchema(str(schema_path)).response("/markets", "get", data)
        pg = data["pagination"]
        if pg["hasMore"] != (index < len(receipts) - 1):
            raise SchemaMismatch("SEMANTIC_CATALOG_PAGE_CHAIN_INCOMPLETE")
        if pg["hasMore"]:
            cursor = pg.get("nextCursor")
            if not cursor or cursor in cursors or not data["data"]:
                raise SchemaMismatch("SEMANTIC_CATALOG_CURSOR_LOOP")
            cursors.add(cursor)
        elif pg.get("nextCursor") is not None:
            raise SchemaMismatch("SEMANTIC_CATALOG_TERMINAL_CONFLICT")
        raw.extend(data["data"])
        if index == len(receipts) - 1 and pg.get("total", len(raw)) != len(raw):
            raise SchemaMismatch("SEMANTIC_CATALOG_TOTAL_MISMATCH")
    return raw, report["tournament_id"], receipts, report.get("account_id_sha256")


def shared_snapshot(path, markets, tid, account_hash, at, max_age):
    raw = Path(path).read_bytes()
    envelope = json.loads(raw)
    payload = envelope["payload"]
    if envelope["digest"] != content_hash(payload):
        raise SchemaMismatch("SEMANTIC_SHARED_SOURCE_CORRUPTED")
    if (
        not account_hash
        or hashlib.sha256(payload["account_id"].encode()).hexdigest() != account_hash
    ):
        raise SchemaMismatch("SEMANTIC_SHARED_ACCOUNT_MISMATCH")
    mapped = [m for m in markets if m["exchange_id"]]
    rows = [
        {"api_market_id": m["sig_market_id"], "exchange_ids": [m["exchange_id"]]} for m in mapped
    ]
    collector = PassiveCollector(path, tid, payload["account_id"], rows, max_age_seconds=max_age)
    # Validate the exact envelope already archived; a publisher can replace its path meanwhile.
    receipt_path = PRIVATE / "shared" / (uuid4().hex + ".json")
    private_write(receipt_path, raw)
    collector.path = receipt_path
    books, state = collector.monitoring_snapshot()
    digest = hashlib.sha256(raw).hexdigest()
    quality, normalized = [], {}
    for m in markets:
        book = books.get(m["exchange_id"])
        reasons = list(state.get("pending_reasons", {}).get(m["sig_market_id"], []))
        if not book:
            reasons.append("MISSING_REAL_BOOK")
        quality.append(
            {
                "local_market_key": m["local_market_key"],
                "exchange_id": m["exchange_id"],
                "quality": "GAP" if reasons else "HEALTHY",
                "reasons": reasons,
                "received_at": book.get("received_at") if book else None,
                "source_time": book.get("asOf", {}).get("at")
                if book and book.get("asOf")
                else None,
                "validation_received_at": book.get("validation_received_at") if book else None,
                "raw_sha256": digest,
            }
        )
        if book:
            normalized[m["exchange_id"]] = {
                **book,
                "quality": "GAP" if reasons else "HEALTHY",
                "data_provenance": "OFFICIAL_REAL",
                "raw_sha256": digest,
                "own_orders_known": False,
                # Same-contract NO prices are documented complements of YES bids.
                # Ownership is still unknown: these aggregate levels cannot pass sweep().
                "no_asks": sorted(
                    [
                        {
                            "price": str(1 - number(level["price"], probability=True)),
                            "quantity": str(number(level["quantity"])),
                            "own": None,
                        }
                        for level in book["bids"]
                    ],
                    key=lambda level: number(level["price"]),
                ),
                "no_side_mapping_evidence": "OFFICIAL_SAME_CONTRACT_BINARY_COMPLEMENT",
            }
            # Aggregate shared books include own orders without identity per level.
            # Do not invent an empty account-order list to claim external liquidity.
    manifest = {
        "layer": "REAL_READ",
        "consumer_http_requests": 0,
        "capture_received_at": at.isoformat(),
        "producer_published_at": payload["published_at"],
        "raw_path": str(receipt_path),
        "raw_sha256": digest,
        "subscription_status": state["subscription_status"],
        "market_count": len(markets),
        "books_observed": len(books),
        "healthy_markets": sum(q["quality"] == "HEALTHY" for q in quality),
        "pending_markets": sum(q["quality"] != "HEALTHY" for q in quality),
        "own_order_exclusion": "NOT_VERIFIED",
        "source_time_is_receipt_time": False,
        "historical_l2_backfilled": False,
        "real_writes": 0,
    }
    return normalized, quality, manifest


def historical_inventory():
    """Inspect existing manifest-backed history, without scanning the runtime tree."""
    artifacts: list[dict] = []
    for name in [
        "reports/history/REAL_READ.json",
        "reports/history/daily-range-study-2026-10-14/COMBINED_REAL_READ.json",
    ]:
        path = Path(name)
        if not path.exists():
            artifacts.append({"path": name, "status": "NOT_FOUND"})
            continue
        body = path.read_bytes()
        data = json.loads(body)
        source_reports = [data]
        if "run_ids" in data and not data.get("request_receipts"):
            for source_path in [
                path.with_name("REAL_READ.json"),
                *path.parent.glob("RECOVERY_*.json"),
            ]:
                if source_path.exists():
                    source_report = json.loads(source_path.read_text())
                    if source_report.get("run_id") in data["run_ids"]:
                        source_reports.append(source_report)
        receipts = [
            r
            for source_report in source_reports
            for r in source_report.get("request_receipts", [])
            if r.get("capability") in {"price_history", "public_trades"}
        ]
        verified, failed = 0, []
        for receipt in receipts:
            raw_path = receipt.get("private_evidence_path")
            if not raw_path or not Path(raw_path).is_file():
                failed.append("RAW_RECEIPT_MISSING")
            elif (
                hashlib.sha256(Path(raw_path).read_bytes()).hexdigest()
                != receipt["response_sha256"]
            ):
                failed.append("RAW_RECEIPT_HASH_MISMATCH")
            else:
                verified += 1
        artifacts.append(
            {
                "path": name,
                "report_sha256": hashlib.sha256(body).hexdigest(),
                "layer": data.get("layer"),
                "status": data.get("status"),
                "resolution": data.get("resolution"),
                "markets_reported": len(data.get("markets", [])),
                "window_from": data.get("window_from_inclusive"),
                "window_to": data.get("window_to_exclusive"),
                "history_tape_receipts_verified": verified,
                "receipt_failures": failed,
                "historical_l2_proven": False,
                "research_use": "TRADED_PRICE_DESCRIPTIVE_HISTORY_NOT_EXECUTABLE_BBO",
            }
        )
    return {
        "artifacts": artifacts,
        "synchronous_bbo_dataset_validated": False,
        "historical_data_does_not_imply_pit_receipt_availability": True,
        "event_level_effective_samples": None,
        "cost_adjusted_return": None,
    }


def append_manifest(path, value):
    import fcntl

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX)
        stream.write(json.dumps(value, ensure_ascii=False, default=str) + "\n")
        stream.flush()
        os.fsync(stream.fileno())


def audit(args):
    output, at = Path(args.output), datetime.now(timezone.utc)
    local = inputs(args.source)
    raw, tid, receipts, account_hash = captured_catalog(args.catalog)
    markets, mapping = map_catalog(local, raw, tid, receipts)
    rules = latest_rules(args.rules, at)
    for market in markets:
        version = rules.get(market["sig_market_id"])
        market["rules_hash"] = version["rules_hash"] if version else None
        market["rules_status"] = "RULES_FETCHED" if version else "MISSING_OFFICIAL_INFO"
        if version:
            market["official_contract_details"] = version.get("contract_details")
            market["selected_party_label_raw"] = version.get("selected_party_label_raw")
            market["fusion_ticket_multiple_yes"] = version.get("fusion_ticket_multiple_yes", False)
    reviews = json.loads(Path(args.reviews).read_text()) if args.reviews else {}
    from sigbot.strategies.semantic_sig_v1.nominees import load_candidate_audits

    candidate_evidence = load_candidate_audits(
        getattr(args, "candidate_audits", []), markets, rules, at
    )
    queue = []
    for race, members in groups(markets).items():
        status, reasons = review_status(members, rules, reviews.get(race), at)
        queue.append(
            {
                "race_id": race,
                "verification_status": status,
                "reasons": reasons,
                "priority": "P0" if len(members) == 3 or members[0]["scope"] == "CONTROL" else "P1",
                "normal_binary_only": True,
                "exhaustivity": "UNKNOWN",
                "independent_caucus": "UNKNOWN",
                "trade_enabled": False,
                "candidate_evidence_status": candidate_evidence.get(race, {}).get(
                    "status", "NOT_PROVIDED"
                ),
                "source_rules_hashes": {m["sig_market_id"]: m["rules_hash"] for m in members},
                "official_party_labels": {
                    m["local_market_key"]: m.get("selected_party_label_raw") for m in members
                },
            }
        )
    books, quality, manifest = {}, [], {"status": "NOT_COLLECTED", "real_writes": 0}
    if getattr(args, "with_account_orders", False):
        from sigbot.strategies.semantic_sig_v1.execution_inputs import capture_account_books

        books, quality, manifest = capture_account_books(args, markets, tid, account_hash)
        at = datetime.now(timezone.utc)
    elif Path(args.shared_source).exists():
        books, quality, manifest = shared_snapshot(
            args.shared_source, markets, tid, account_hash, at, args.max_age
        )
    write_json(
        output / "market_registry.json",
        {
            "data_provenance": "REAL_CATALOG_WITH_OLD_TITLE_SEEDS",
            "as_of_utc": at.isoformat(),
            "tournament_id": tid,
            "mapping": mapping,
            "markets": markets,
        },
    )
    write_json(
        output / "rules_versions.json",
        {
            "source": "OFFICIAL_INFO_ONLY",
            "versions": list(rules.values()),
            "missing_contracts": [
                m["sig_market_id"] for m in markets if m["sig_market_id"] not in rules
            ],
            "catalog_payload_hash_is_rules_hash": False,
        },
    )
    write_json(output / "relation_review_queue.json", queue)
    write_json(output / "candidate_resolution_evidence.json", candidate_evidence)
    write_json(output / "data_manifest.json", manifest)
    append_manifest(output / "data_manifest_history.jsonl", manifest)
    write_json(output / "data_quality.json", quality)
    write_json(output / "historical_data_inventory.json", historical_inventory())
    write_json(output / "seat_coverage.json", seat_coverage(markets))
    same_race_scan = scan(
        markets,
        books,
        rules,
        reviews,
        at=at,
        quantity=args.quantity,
        fees=args.fees,
        cash=args.cash,
        buffer=args.risk_buffer,
        max_age=args.max_age,
        max_skew=args.max_skew,
        candidate_audits=candidate_evidence,
    )
    write_json(output / "same_race_scan.json", same_race_scan)
    statistical = read_csv(Path(args.source) / "data/same_state_candidates.csv")
    for name in ["relation_diagnostics", "lead_lag_validation", "economic_capacity"]:
        rows = []
        for edge in statistical:
            row = {
                "edge_id": edge["edge_id"],
                "source_race": edge["source_race"],
                "target_race": edge["target_race"],
                "semantic_candidate": True,
                "empirically_validated": False,
                "status": "NOT_EVALUATED",
                "reason": "SEPARATE_S3_VALIDATION_NOT_IMPORTED_BY_THIS_RULES_AUDIT",
            }
            fields = {
                "relation_diagnostics": [
                    "overlap_healthy_seconds",
                    "effective_event_count",
                    "lagged_correlation",
                    "controlled_beta",
                    "multiple_test_adjustment",
                ],
                "lead_lag_validation": [
                    "train_start",
                    "train_end",
                    "test_start",
                    "test_end",
                    "incremental_oos_score",
                    "net_bid_ask_return",
                    "direction_stability",
                ],
                "economic_capacity": [
                    "signal_count",
                    "executable_count",
                    "executable_quantity",
                    "partial_fill_rate",
                    "capital_lock_seconds",
                    "cost_adjusted_edge",
                ],
            }[name]
            row.update(dict.fromkeys(fields))
            rows.append(row)
        write_json(output / (name + ".json"), rows)
    relation_reasons = sorted({reason for q in queue for reason in q["reasons"]})
    scan_reasons = sorted({reason for row in same_race_scan for reason in row["rejection_reasons"]})
    module_reasons = {
        "S0": relation_reasons,
        "S1": scan_reasons,
        "S2": [*relation_reasons, "PAYOFF_ROUTE_AND_EXECUTABLE_COST_REVIEW_REQUIRED"],
        "S3": ["SEPARATE_S3_MODEL_AND_EXECUTION_VALIDATION_REQUIRED"],
        "S4": ["SENATE_COMPLETE_COVERAGE_CAUCUS_AND_CONTROL_MODEL_REQUIRED"],
        "S5": ["HOUSE_COMPLETE_COVERAGE_BOUNDARIES_AND_CONTROL_MODEL_REQUIRED"],
        "S6": ["TIMESTAMPED_DEDUPLICATED_NEWS_AND_INCREMENT_VALIDATION_REQUIRED"],
        "S7": ["SEMANTIC_FAIR_VALUE_INPUT_NOT_VALIDATED_EXISTING_MM_ADMISSION_UNCHANGED"],
    }
    rejected = [q for q in queue if q["verification_status"] == "REJECTED_BY_RULES"]
    write_json(
        output / "hypothesis_decisions.json",
        [
            {
                "race_id": q["race_id"],
                "decision": "REJECT",
                "claim_scope": "UNCONDITIONAL_MUTUAL_EXCLUSION_AND_K_MINUS_ONE_NO_PAYOFF_BOUND",
                "reason": "OFFICIAL_RULES_ALLOW_MULTIPLE_PARTY_YES",
                "source_rules_hashes": q["source_rules_hashes"],
                "actual_fusion_candidate_observed": None,
                "trade_enabled": False,
            }
            for q in rejected
        ],
    )
    decisions: list[dict] = [
        {
            "strategy_id": f"S{i}",
            "decision": "NEED_MORE_DATA",
            "real_pnl": None,
            "evidence": [
                "market_registry.json",
                "data_manifest.json",
                "same_race_scan.json",
                "seat_coverage.json",
            ],
            "reasons": module_reasons[f"S{i}"],
            "admission_scope": "THIS_MODULE_ONLY",
            "trade_enabled": False,
        }
        for i in range(8)
    ]
    for i in (0, 1, 2):
        decisions[i]["not_required_for_this_module"] = [
            "S3_TRAIN_VALIDATION_TEST_ROW_COUNTS",
            "S3_LEAD_LAG_SIGNIFICANCE",
            "OHIO_AND_UNLISTED_HOUSE_SEAT_MODEL",
        ]
    decisions[1].update(
        candidate_conditioned_payoffs_evaluated=sum(
            r.get("candidate_conditioned_min_payout") is not None for r in same_race_scan
        ),
        computation_status="READ_ONLY_PAYOFF_SCAN_COMPLETE",
        payoff_bounds_evaluated=sum(r["normal_min_payout"] is not None for r in same_race_scan),
        scan_objects=len(same_race_scan),
        historical_l2_required_for_current_payoff_scan=False,
        current_execution_requirements="FRESH_EXTERNAL_DEPTH_COSTS_BUDGET_AND_SHARED_OMS",
        current_scan_rejections=dict(
            Counter(reason for r in same_race_scan for reason in r["rejection_reasons"])
        ),
    )
    if rejected:
        decisions[1].update(
            decision="REJECT",
            claim_scope="S1_UNCONDITIONAL_MUTEX_BUNDLE_AS_CURRENTLY_DESIGNED",
            reasons=[
                "OFFICIAL_RULES_ALLOW_MULTIPLE_PARTY_YES",
                "NO_PROOF_OF_EXCLUSIVE_WINNING_AFFILIATIONS",
            ],
            evidence=[
                "same_race_scan.json",
                "hypothesis_decisions.json",
                "rules_versions.json",
            ],
        )
        decisions[2]["reasons"].append("UNCONDITIONAL_D_YES_SUBSET_R_NO_NOT_PROVEN")
    write_json(
        output / "decision.json",
        {
            "final_decision": "NEED_MORE_DATA",
            "aggregate_is_execution_gate": False,
            "module_admission_is_independent": True,
            "strategies": decisions,
            "as_of_utc": at.isoformat(),
            "synthetic_profitability_evidence": False,
            "real_writes": 0,
        },
    )
    summary = {
        "status": "RESEARCH_AUDIT_COMPLETE_WITH_BLOCKERS",
        "mapped": mapping["mapped"],
        "contracts": len(local),
        "underlying_questions": len(groups(local)),
        "states": len({m["state"] for m in local} - {"US"}),
        "rules_available": len(rules),
        "relation_statuses": dict(Counter(q["verification_status"] for q in queue)),
        "data": manifest,
        "decision": "NEED_MORE_DATA",
        "real_writes": 0,
        "oms_integration": "RESEARCH_OUTPUT_ONLY_NO_ORDER_QUEUE",
        "output": str(output),
    }
    write_json(output / "audit_summary.json", summary)
    return summary


def run(args):
    if args.action == "candidate-audit":
        from sigbot.strategies.semantic_sig_v1.nominees import audit_candidates

        return audit_candidates(
            args.input,
            args.registry,
            args.output,
            nomination_evidence=getattr(args, "nomination_evidence", None),
            previous_audit=getattr(args, "previous_audit", None),
        ), 0
    if args.action == "history-relations":
        from sigbot.strategies.semantic_sig_v1.history_relations import research

        return research(
            args.study,
            args.registry,
            args.previous_model,
            args.output,
            factor_policy=args.factor_policy,
        ), 0
    if args.action == "replay-costs":
        from sigbot.strategies.semantic_sig_v1.replay import depth_scenario

        rows = json.loads(Path(args.input).read_text())
        result = [depth_scenario(row) for row in rows]
        write_json(Path(args.output) / "depth_cost_scenarios.json", result)
        return {
            "evaluated": sum(r["status"] != "NOT_EVALUATED" for r in result),
            "not_evaluated": sum(r["status"] == "NOT_EVALUATED" for r in result),
            "real_pnl": None,
        }, 0
    if args.action == "capture":
        from sigbot.strategies.semantic_sig_v1.timeseries import capture

        result = capture(
            args.shared_source,
            args.output,
            seconds=args.seconds,
            interval=args.interval,
            market_ids=set(filter(None, args.market_ids.split(","))),
        )
        return {
            "status": "CAPTURE_COMPLETE",
            "samples": len(result["samples"]),
            "all_samples_healthy_connection": result["all_samples_healthy_connection"],
            "all_markets_contiguous": result["all_markets_contiguous"],
            "real_writes": 0,
        }, 0
    if args.action == "dataset":
        from sigbot.strategies.semantic_sig_v1.timeseries import build_dataset

        result = build_dataset(args.archive, args.registry, args.output, max_bytes=args.max_bytes)
        return {
            "status": "DATASET_BUILT",
            "frames": result["frames"],
            "counts": result["counts"],
            "real_writes": 0,
        }, 0
    if args.action == "validate":
        from sigbot.strategies.semantic_sig_v1.model import validate

        result = validate(args.dataset_manifest, args.output, set(args.targets.split(",")))
        return {
            "decision": result["decision"],
            "candidate_edges": result["candidate_edges"],
            "live_eligible": result["live_eligible"],
            "real_writes": 0,
        }, 0
    if args.action in {"live-preflight", "execute"}:
        from sigbot.strategies.semantic_sig_v1.live import run_live

        return run_live(args)
    if args.action == "collect":
        return collect(args)
    if args.action == "import-rules":
        value = record_rules(args.rules, json.loads(Path(args.input).read_text()))
        return {
            "status": "RULES_FETCHED",
            "rules_hash": value["rules_hash"],
            "trade_enabled": False,
        }, 0
    if args.action == "import-public-rules":
        from sigbot.strategies.semantic_sig_v1.official_rules import import_public_rules

        bundle = json.loads(Path(args.input).read_text())
        result = import_public_rules(
            bundle["market_registry"],
            bundle["page_receipts"],
            bundle["renderer_receipt"],
            bundle["settlement_policy"],
            args.rules,
            args.output,
        )
        return result, 2 if result["failed"] else 0
    return audit(args), 0
