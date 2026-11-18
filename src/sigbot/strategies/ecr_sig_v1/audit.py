"""Bounded explicit official reads for correspondence and account acceptance."""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from pathlib import Path

from sigbot.accountcheck import checkpoint, persist_checkpoint
from sigbot.errors import AuthorizationRequired, SchemaMismatch
from sigbot.readcheck import load_key
from sigbot.storage import Store
from sigbot.strategies.ecr_sig_v1.correspondence import correspondences, sig_predicate
from sigbot.strategies.ecr_sig_v1.observer import ROOT, ContinuousReads, atomic_json
from sigbot.strategies.ecr_sig_v1.publicdata import utcnow
from sigbot.strategies.prc_sig_v1.catalog import catalog_rows, refresh_reference


def bootstrap(reads):
    account = reads.call("account_identity")
    if not account["id"] or account["id"].startswith("SIM-"):
        raise AuthorizationRequired("REAL_ACCOUNT_REQUIRED")
    reads.reader.config.expected_account_id = account["id"]
    cups, _ = reads.check(
        "tournament_list",
        lambda: reads.pages("tournament_list", query={"status": "any", "limit": 100, "offset": 0}),
    )
    matches = [c for c in cups if c["slug"] == reads.slug]
    if len(matches) != 1 or matches[0]["isPendingEnrolment"]:
        raise AuthorizationRequired("ACCOUNT_TOURNAMENT_AMBIGUOUS_OR_PENDING")
    reads.reader.config.expected_tournament_id = matches[0]["id"]
    reads.reader.config.expected_tournament_slug = reads.slug
    return account["id"], matches[0]["id"]


def run_audit(args, contract_file, *, account_only=False):
    if not args.authorize_read:
        raise AuthorizationRequired("EXPLICIT_INPUT_AUDIT_READ_REQUIRED")
    if not account_only and args.max_node_reads < 0:
        raise ValueError("negative node bound")
    output = Path(
        args.output
        or (
            "reports/ACCOUNT_CHECKPOINT.json"
            if account_only
            else "reports/strategies/ecr_sig_v1/INPUT_AUDIT.json"
        )
    )
    if "reports" not in output.parts or output.suffix != ".json":
        raise ValueError("audit output must be a reports JSON file")
    reference = refresh_reference(output.parent / "readiness")
    reads = ContinuousReads(load_key(args.env_file), contract_file)
    store = None
    try:
        aid, tid = bootstrap(reads)
        result, data = checkpoint(reads)
        ROOT.mkdir(parents=True, exist_ok=True, mode=0o700)
        atomic_json(reads.private / "account-checkpoint.json", {"result": result, "data": data})
        if args.checkpoint_database_url:
            store = Store(args.checkpoint_database_url, "readonly")
            persist_checkpoint(store, aid, result, data, utcnow())
        public_account = {
            k: v
            for k, v in result.items()
            if k
            not in {"journal", "positions", "projected_cash", "remote_cash", "journal_cash_total"}
        }
        public_account["position_cards"] = len(result["positions"])
        report = {
            "layer": "REAL_READ",
            "run_id": reads.run_id,
            "observed_at": utcnow().isoformat(),
            "account_id_sha256": hashlib.sha256(aid.encode()).hexdigest(),
            "tournament_id": tid,
            "account": public_account,
            "reference": reference,
            "checkpoint_database_import": bool(store),
            "production_oms_ledger_import": False,
            "real_orders": 0,
            "real_cancels": 0,
            "account_modifications": 0,
        }
        if not account_only:
            markets, _ = reads.check(
                "market_list",
                lambda: reads.pages(
                    "market_list", query={"tournamentId": tid, "status": "any", "limit": 100}
                ),
            )
            rows = catalog_rows(markets, tid)
            eligible = [r for r in rows if r["event_candidate"]["office"] != "CHAMBER_CONTROL"]
            bound = args.max_node_reads or len(eligible)
            nodes, captured = {}, {}
            for row in eligible[:bound]:
                node = reads.call(
                    "market_nodes",
                    path_params={"id": row["api_market_id"]},
                    query={"tournamentId": tid},
                )
                if node["market_id"] != row["api_market_id"]:
                    raise SchemaMismatch("AUDIT_NODE_MARKET_MISMATCH")
                nodes[row["api_market_id"]] = node
                captured[row["api_market_id"]] = {
                    "nodes": node,
                    "observed_at": utcnow().isoformat(),
                    "receipt": dict(reads.requests[-1]),
                }
                # Persist incrementally. A later HTTP failure does not erase evidence.
                atomic_json(ROOT / "audited-nodes.json", captured)
            external_file = ROOT / "external-candidates.json"
            external = json.loads(external_file.read_text()) if external_file.exists() else []
            comparisons = correspondences(rows, nodes, external, tid)
            predicates = [
                sig_predicate(row, nodes[row["api_market_id"]], tid)
                for row in eligible
                if row["api_market_id"] in nodes
            ]
            position_markets = {
                p["exchangeId"]: p["marketId"]
                for p in data["positions"]["positions"]
                if p["quantity"]
            }
            event_map = {
                r["api_market_id"]: sig_predicate(r, nodes[r["api_market_id"]], tid)[0]
                for r in rows
                if r["api_market_id"] in nodes
            }
            exposure: dict = {}
            unmapped = []
            for instrument, pos in result["positions"].items():
                exchange = instrument.split(":", 1)[0]
                event = event_map.get(position_markets.get(exchange))
                if not event:
                    unmapped.append(instrument)
                    continue
                from decimal import Decimal

                key = event["event_key"]
                exposure[key] = str(Decimal(exposure.get(key, "0")) + Decimal(pos["cost"]))
            report.update(
                catalog_complete=True,
                catalog_markets=len(rows),
                control_observe_only=len(rows) - len(eligible),
                nodes_observed=len(nodes),
                node_coverage_complete=len(nodes) == len(eligible),
                structured_event_cards=sum(p is not None for p, _ in predicates),
                structured_event_rejections=dict(
                    Counter(reason for p, reason in predicates if p is None)
                ),
                verified_predicate_cards=len(
                    {c["api_market_id"] for c in comparisons if c["unique_operator_match"]}
                ),
                settlement_equivalent_cards=len(
                    {c["api_market_id"] for c in comparisons if c["settlement_equivalent"]}
                ),
                correspondence_rejections=dict(
                    Counter(c["reason"] for c in comparisons if not c["unique_operator_match"])
                ),
                comparisons=comparisons,
                account_event_exposure=exposure,
                occupied_account_events=len(exposure),
                unmapped_position_instruments=unmapped,
                actual_candidates_qualified=0,
                external_universe_complete=False,
            )
        report.update(
            authenticated_get_attempts=reads.total_attempts,
            responses_by_capability=dict(reads.count_by_capability),
            private_evidence_directory=str(reads.private),
            finished_at=utcnow().isoformat(),
        )
        report["status"] = (
            "PASS_ACCOUNT_READ_PROJECTION"
            if account_only and result["status"] == "PASS"
            else (
                "INPUT_AUDIT_COMPLETE_WITH_TRADING_GAPS"
                if not account_only and report["node_coverage_complete"]
                else "PARTIAL"
            )
        )
        atomic_json(output, report)
        return report
    finally:
        if store:
            store.engine.dispose()
        reads.reader.close()
        reads.key = ""
