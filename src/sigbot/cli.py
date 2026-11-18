from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path

from pydantic import ValidationError
from sqlalchemy.exc import SQLAlchemyError

from sigbot.catalog import Catalog
from sigbot.config import Config
from sigbot.contracts import Contract
from sigbot.domain import Clock, Intent
from sigbot.errors import AuthorizationRequired, ContractNotVerified, SigbotError
from sigbot.operations import Controls, watchdog
from sigbot.simulation import replay
from sigbot.storage import Store


def parser() -> argparse.ArgumentParser:
    from sigbot.strategies.semantic_sig_v1.registry import SOURCE

    p = argparse.ArgumentParser(
        prog="sigbot", description="SIG strategy and order tools, offline by default"
    )
    p.add_argument("--config", help="Strict application config; never loads .env")
    p.add_argument("--database-url", help="Local demo DB or explicitly selected PostgreSQL")
    commands = p.add_subparsers(dest="command", required=True)
    semantic = commands.add_parser(
        "semantic", help="Election semantic research and explicitly authorized shared-OMS execution"
    ).add_subparsers(dest="action", required=True)
    for action in (
        "audit",
        "collect",
        "scan",
        "import-rules",
        "import-public-rules",
        "capture",
        "dataset",
        "validate",
        "live-preflight",
        "execute",
        "replay-costs",
        "history-relations",
        "candidate-audit",
    ):
        command = semantic.add_parser(action)
        command.add_argument("--source", default=str(SOURCE))
        command.add_argument("--output", default="reports/strategies/semantic_sig_v1")
        command.add_argument(
            "--catalog", default="reports/strategies/semantic_sig_v1/catalog_receipt_manifest.json"
        )
        command.add_argument(
            "--shared-source", default="runtime/readonly/shared/sig-depth.current.json"
        )
        command.add_argument("--rules", default="runtime/readonly/strategies/semantic_sig_v1/rules")
        command.add_argument("--reviews")
        command.add_argument("--quantity", default="1")
        command.add_argument("--fees")
        command.add_argument("--cash")
        command.add_argument("--risk-buffer", default="0")
        command.add_argument("--max-age", type=float, default=5)
        command.add_argument("--max-skew", type=float, default=2)
        if action in {"audit", "scan"}:
            command.add_argument("--candidate-audits", nargs="+", default=[])
            command.add_argument("--with-account-orders", action="store_true")
            command.add_argument("--authorize-read", action="store_true")
            command.add_argument("--env-file", default=".env")
            command.add_argument("--max-requests", type=int, default=12)
            command.add_argument("--seconds", type=int, default=60)
            command.add_argument(
                "--account-contract",
                default="contracts/strategies/mm_sig_v1.execution.reviewed-2026-10-17.json",
            )
        if action == "candidate-audit":
            command.add_argument("--input", required=True)
            command.add_argument("--nomination-evidence")
            command.add_argument("--previous-audit")
            command.add_argument(
                "--registry", default="reports/strategies/semantic_sig_v1/market_registry.json"
            )
        if action == "capture":
            command.add_argument("--seconds", type=int, default=60)
            command.add_argument("--interval", type=float, default=2)
            command.add_argument(
                "--market-ids",
                default="",
                help="Expiring shared-source priorities; preserves other consumers",
            )
        if action == "dataset":
            command.add_argument(
                "--archive", default="runtime/readonly/strategies/ecr_sig_v1/quotes.jsonl"
            )
            command.add_argument(
                "--registry", default="reports/strategies/semantic_sig_v1/market_registry.json"
            )
            command.add_argument("--max-bytes", type=int)
        if action == "validate":
            command.add_argument("--dataset-manifest", required=True)
            command.add_argument("--targets", default="1036,1075,978")
        if action == "history-relations":
            command.set_defaults(output="reports/strategies/semantic_sig_v1/history_relations")
            command.add_argument(
                "--factor-policy",
                choices=("full-state", "national-office"),
                default="full-state",
                help="Fixed baseline features; national-office is a separate exploratory protocol",
            )
            command.add_argument("--study", default="reports/history/daily-range-study-2026-10-14")
            command.add_argument(
                "--registry", default="reports/strategies/semantic_sig_v1/market_registry.json"
            )
            command.add_argument(
                "--previous-model",
                default="reports/strategies/semantic_sig_v1/live_implementation_20261014/final_validation/model_validation.json",
            )
        if action in {"live-preflight", "execute"}:
            command.add_argument("--gateway-settings", required=True)
            command.add_argument("--model-validation", required=True)
            command.add_argument("--authorize-read", action="store_true")
            command.add_argument("--authorize-submit", action="store_true")
            command.add_argument("--env-file", default=".env")
            command.add_argument("--max-requests", type=int, default=12)
            command.add_argument("--seconds", type=int, default=180)
            command.add_argument(
                "--audit-costs",
                action="store_true",
                help="Current official book/order evidence; does not approve missing fees",
            )
            if action == "execute":
                command.add_argument("--signal-file")
        if action == "collect":
            command.add_argument("--authorize-read", action="store_true")
            command.add_argument("--env-file", default=".env")
            command.add_argument("--key-name", default="api_key")
            command.add_argument("--tournament-slug", default="midterm-elections")
            command.add_argument(
                "--contract",
                default="contracts/strategies/semantic_sig_v1.reads.json",
            )
            command.add_argument("--max-requests", type=int, default=12)
            command.add_argument("--max-pages", type=int, default=10)
            command.add_argument("--seconds", type=int, default=180)
        if action in {"import-rules", "import-public-rules"}:
            command.add_argument("--input", required=True)
        if action == "replay-costs":
            command.add_argument("--input", required=True)
    doctor = commands.add_parser("doctor")
    doctor.add_argument("--mode", default="offline", choices=["offline"])
    contract = commands.add_parser("contract").add_subparsers(dest="action", required=True)
    check = contract.add_parser("check")
    check.add_argument("--mode", default="read-only")
    strategy = commands.add_parser("strategy").add_subparsers(dest="action", required=True)
    strategy.add_parser("list")
    mm_scan = strategy.add_parser(
        "scan", help="Finite MM official GET-only data scan; never trades"
    )
    mm_scan.add_argument("--strategy", required=True, choices=["mm_sig"])
    mm_scan.add_argument("--version", default="1.0", choices=["1.0"])
    mm_scan.add_argument("--authorize-read", action="store_true")
    mm_scan.add_argument("--env-file", default=".env")
    mm_scan.add_argument("--tournament-slug", default="midterm-elections")
    mm_scan.add_argument("--sample-markets", type=int, default=3)
    mm_scan.add_argument("--samples", type=int, default=3)
    mm_scan.add_argument("--max-requests", type=int, default=40)
    mm_scan.add_argument("--seconds", type=int, default=90)
    mm_scan.add_argument("--output")
    acquisition = strategy.add_parser(
        "collect-data", help="Bounded anonymous original poll reads; no SIG key"
    )
    acquisition.add_argument("--strategy", required=True, choices=["prc_sig"])
    acquisition.add_argument("--version", default="1.0", choices=["1.0"])
    acquisition.add_argument("--authorize-public-read", action="store_true")
    acquisition.add_argument(
        "--profile12-proxy",
        action="store_true",
        help="Use operator-verified isolated profile 12 port 17890; requires prior route authorization",
    )
    acquisition.add_argument("--per-source", type=int, default=3)
    acquisition.add_argument("--profile12-port", type=int, default=17890, choices=[17890, 19881])
    acquisition.add_argument("--max-requests", type=int, default=20)
    acquisition.add_argument("--seconds", type=int, default=120)
    acquisition.add_argument("--output")
    data_status = strategy.add_parser(
        "data-status", help="Offline audit of original polls and shared SIG receipts"
    )
    data_status.add_argument("--strategy", required=True, choices=["prc_sig", "ecr_sig"])
    data_status.add_argument("--version", default="1.0", choices=["1.0"])
    data_status.add_argument("--output")
    poll_import = strategy.add_parser(
        "import-poll", help="Receipt-bound local poll questions; never approves trades"
    )
    poll_import.add_argument("--strategy", required=True, choices=["prc_sig"])
    poll_import.add_argument("--version", default="1.0", choices=["1.0"])
    poll_import.add_argument("--input", required=True)
    poll_import.add_argument(
        "--poll-database-url", help="Explicit migrated PostgreSQL projection; no implicit DB setup"
    )
    poll_import.add_argument("--output")
    observer = strategy.add_parser("observe", help="Continuous official ECR reads; never trades")
    observer.add_argument("--strategy", required=True, choices=["ecr_sig", "mm_sig"])
    observer.add_argument("--version", default="1.0", choices=["1.0", "1.1"])
    observer.add_argument("--authorize-read", action="store_true")
    observer.add_argument("--env-file", default=".env")
    observer.add_argument(
        "--fee-approval-file", help="Existing operator fee acceptance; observation only"
    )
    observer.add_argument(
        "--realtime-contract-file", default="contracts/realtime.auto-2026-10-08.json"
    )
    observer.add_argument(
        "--shared-executor-config", help="Queue proposals to the sole account executor"
    )
    observer.add_argument(
        "--review-input", help="Verified ECR runtime review input; no historical LOB"
    )
    observer.add_argument(
        "--public-route", choices=["direct", "rabisu", "rabisu-relay"], default="rabisu-relay"
    )
    observer.add_argument(
        "--public-proxy",
        default="socks5h://127.0.0.1:18449",
        help="Existing Rabisu relay for public reads only; never switches main ClashSub",
    )
    observer.add_argument("--rabisu-node", default="Rabisu-LA")
    observer.add_argument(
        "--rabisu-config", help="Local cached Rabisu YAML; never changes main ClashSub"
    )
    observer.add_argument(
        "--duration-seconds",
        type=int,
        default=0,
        help="0 observes until stopped; positive values bound the run",
    )
    input_audit = strategy.add_parser(
        "audit-inputs", help="Official rule correspondence and account reconstruction; GET only"
    )
    automatic = strategy.add_parser(
        "auto", help="Finite ECR entry, fills, exits and recovery through the sole Executor"
    )
    automatic.add_argument("--strategy", required=True, choices=["ecr_sig"])
    automatic.add_argument(
        "--input",
        required=True,
        help="Synthetic OFFLINE snapshot or operator-reviewed real template",
    )
    automatic.add_argument("--authorize-read", action="store_true")
    automatic.add_argument("--authorize-submit", action="store_true")
    automatic.add_argument("--approval-file")
    automatic.add_argument(
        "--inspect",
        action="store_true",
        help="Local review/binding report only; no credentials, network or orders",
    )
    automatic.add_argument(
        "--resume",
        action="store_true",
        help="Explicit startup resume after reconciliation; no implicit restart resume",
    )
    automatic.add_argument("--env-file", default=".env")
    automatic.add_argument("--duration-seconds", type=int, default=600)
    automatic.add_argument(
        "--public-route", choices=["direct", "rabisu", "rabisu-relay"], default="rabisu-relay"
    )
    automatic.add_argument("--public-proxy", default="socks5h://127.0.0.1:18449")
    automatic.add_argument("--rabisu-node", default="Rabisu-LA")
    automatic.add_argument("--rabisu-config")
    automatic.add_argument(
        "--realtime-contract-file", default="contracts/realtime.auto-2026-10-08.json"
    )
    automatic.add_argument("--output")
    input_audit.add_argument("--strategy", required=True, choices=["ecr_sig"])
    input_audit.add_argument("--version", default="1.0", choices=["1.0"])
    input_audit.add_argument("--authorize-read", action="store_true")
    input_audit.add_argument("--env-file", default=".env")
    input_audit.add_argument(
        "--max-node-reads", type=int, default=0, help="0 reads every non-control contract"
    )
    input_audit.add_argument(
        "--checkpoint-database-url", help="Optional migrated PostgreSQL readonly projection"
    )
    input_audit.add_argument("--output")
    prc_sync = strategy.add_parser("sync-catalog")
    prc_sync.add_argument("--strategy", required=True, choices=["prc_sig"])
    prc_sync.add_argument("--version", default="1.0", choices=["1.0"])
    prc_sync.add_argument("--authorize-read", action="store_true")
    prc_sync.add_argument("--env-file", default=".env")
    prc_sync.add_argument("--key-name", default="api_key")
    prc_sync.add_argument("--tournament-slug", default="midterm-elections")
    prc_sync.add_argument("--max-requests", type=int, default=12)
    prc_sync.add_argument("--max-pages", type=int, default=10)
    prc_sync.add_argument("--seconds", type=int, default=120)
    prc_sync.add_argument("--output")
    prc_sync.add_argument(
        "--catalog-database-url", help="Optional migrated PostgreSQL readonly catalog projection"
    )
    for action in ("plan", "replay"):
        command = strategy.add_parser(action)
        command.add_argument("--strategy", required=True)
        command.add_argument("--version", default="1.0")
        command.add_argument("--strategy-config")
        command.add_argument("--input", required=True)
        command.add_argument("--output")
    stage = strategy.add_parser(
        "stage", help="Stage reviewed real ECR instruments in migrated PostgreSQL; no network"
    )
    stage.add_argument("--strategy", required=True, choices=["ecr_sig"])
    stage.add_argument("--input", required=True)
    execute = strategy.add_parser(
        "execute", help="Finite ECR batch through unified real OMS; human approval required"
    )
    execute.add_argument("--strategy", required=True, choices=["ecr_sig"])
    execute.add_argument("--input", required=True)
    execute.add_argument("--approval-file", required=True)
    execute.add_argument("--authorize-read", action="store_true")
    execute.add_argument("--env-file", default=".env")
    catalog = commands.add_parser("catalog").add_subparsers(dest="action", required=True)
    seed = catalog.add_parser("import-seed")
    seed.add_argument("--file", required=True)
    audit = catalog.add_parser("audit")
    audit.add_argument("--output", default="runtime/offline/catalog")
    sync = catalog.add_parser("sync")
    sync.add_argument("--all-statuses", action="store_true")
    review = catalog.add_parser("review")
    for name in ["market", "rules-hash", "mapping-hash", "operator", "expires-at"]:
        review.add_argument("--" + name, required=True)
    account = commands.add_parser("account").add_subparsers(dest="action", required=True)
    snapshot_command = account.add_parser("snapshot")
    snapshot_command.add_argument("--authorize-read", action="store_true")
    snapshot_command.add_argument("--env-file", default=".env")
    snapshot_command.add_argument("--checkpoint-database-url")
    snapshot_command.add_argument("--output")
    bootstrap = account.add_parser(
        "bootstrap",
        help="Import verified account opening including manual holdings into live PostgreSQL",
    )
    bootstrap.add_argument("--authorize-read", action="store_true")
    bootstrap.add_argument("--env-file", default=".env")
    orders = commands.add_parser("orders").add_subparsers(dest="action", required=True)
    plan = orders.add_parser("plan")
    plan.add_argument("--intent-file", required=True)
    plan.add_argument("--output")
    plan.add_argument("--authorize-read", action="store_true")
    plan.add_argument("--env-file", default=".env")
    submit = orders.add_parser("submit")
    submit.add_argument("--plan", required=True)
    submit.add_argument("--approval-file", required=True)
    cancel = orders.add_parser("cancel")
    cancel.add_argument("--order-id", required=True)
    cancel.add_argument("--approval-file", required=True)
    for command in (submit, cancel):
        command.add_argument("--authorize-read", action="store_true")
        command.add_argument("--env-file", default=".env")
    reconcile = commands.add_parser("reconcile")
    reconcile.add_argument("--scope", default="account")
    reconcile.add_argument("--authorize-read", action="store_true")
    reconcile.add_argument("--env-file", default=".env")
    control = commands.add_parser("control").add_subparsers(dest="action", required=True)
    pause = control.add_parser("pause")
    pause.add_argument("--reason", required=True)
    drain = control.add_parser("drain")
    drain.add_argument("--approval-file", required=True)
    status = commands.add_parser("status")
    status.add_argument("--json", action="store_true")
    commands.add_parser("settle").add_subparsers(dest="action", required=True).add_parser("sync")
    replay_p = commands.add_parser("replay")
    replay_p.add_argument("--fixture", required=True)
    replay_p.add_argument("--output")
    watch = commands.add_parser("watchdog")
    watch.add_argument("--heartbeat", required=True)
    watch.add_argument("--output", required=True)
    return p


def run(args, cfg: Config) -> tuple[dict, int]:
    from sigbot.domain import Mode

    if args.command == "semantic":
        from sigbot.strategies.semantic_sig_v1.research import run as semantic_run

        return semantic_run(args)
    if (
        args.command == "strategy"
        and args.action in {"stage", "execute"}
        or args.command == "account"
        and args.action == "bootstrap"
        or cfg.mode in {Mode.LIVE_CANARY, Mode.LIVE_LIMITED}
        and (
            args.command == "reconcile"
            or args.command == "orders"
            and args.action == "plan"
            and args.authorize_read
            or args.command == "orders"
            and args.action in {"submit", "cancel"}
        )
    ):
        from sigbot.live import run_command

        return run_command(args, cfg)
    if args.command == "account" and args.action == "snapshot":
        from sigbot.strategies.ecr_sig_v1.audit import run_audit

        result = run_audit(args, cfg.contract_file, account_only=True)
        return result, 0 if result["account"]["status"] == "PASS" else 2
    if args.command == "strategy":
        if args.action == "auto":
            from sigbot.strategies.ecr_sig_v1.automatic import run as automatic_run

            return automatic_run(args, cfg)
        if args.action == "audit-inputs":
            from sigbot.strategies.ecr_sig_v1.audit import run_audit

            result = run_audit(args, cfg.contract_file)
            return result, 0 if result["node_coverage_complete"] and result["account"][
                "status"
            ] == "PASS" else 2
        from sigbot.strategies.ecr_sig_v1.inputs import EcrConfig
        from sigbot.strategies.ecr_sig_v1.inputs import Snapshot as EcrSnapshot
        from sigbot.strategies.prc_sig_v1.models import PrcConfig
        from sigbot.strategies.prc_sig_v1.models import Snapshot as PrcSnapshot
        from sigbot.strategies.registry import STRATEGIES, resolve

        scfg: EcrConfig | PrcConfig
        snapshot: EcrSnapshot | PrcSnapshot

        if args.action == "list":
            return {"strategies": [asdict(s) for s in STRATEGIES], "credentials_loaded": False}, 0
        spec = resolve(args.strategy, args.version)
        if (spec.strategy_id, spec.version) == ("mm_sig", "1.1"):
            if args.action != "observe":
                raise ValueError("MM 1.1 requires strategy observe --authorize-read")
            if not args.authorize_read:
                raise AuthorizationRequired("MM_EXPLICIT_REAL_READ_REQUIRED")
            from sigbot.strategies.mm_sig_v1.live.models import load_settings
            from sigbot.strategies.mm_sig_v1.live.runtime import Runtime

            runtime = Runtime(load_settings(), args.env_file)
            import signal

            for signum in (signal.SIGINT, signal.SIGTERM):
                signal.signal(signum, lambda *_: runtime.stop.set())
            result = runtime.run(args.duration_seconds)
            return result, 0
        if spec.strategy_id == "mm_sig":
            from sigbot.strategies.mm_sig_v1.core import plan as mm_plan
            from sigbot.strategies.mm_sig_v1.data import run_scan
            from sigbot.strategies.mm_sig_v1.models import MmConfig, Snapshot
            from sigbot.strategies.mm_sig_v1.replay import replay as mm_replay

            output = spec.check_output(
                args.output or spec.report_directory + "/" + args.action.upper() + ".json"
            )
            if args.action == "scan":
                result = run_scan(args, cfg.contract_file)
                code = 0 if result["status"] == "PASS_READ_ONLY" else 2
            else:
                if cfg.mode.value != "OFFLINE" or cfg.business_network_enabled:
                    raise AuthorizationRequired("MM_LOCAL_STRATEGY_MODE_REQUIRED")
                mm_cfg = MmConfig.model_validate_json(
                    Path(args.strategy_config or spec.config).read_text()
                )
                if args.action == "replay":
                    result = mm_replay(args.input, mm_cfg)
                else:
                    packet = json.loads(Path(args.input).read_text())
                    result = mm_plan(
                        Snapshot.model_validate(packet.get("snapshot", packet)), mm_cfg
                    )
                code = 0 if result.get("status") in {"PASS", "PASS_COMPUTATION_ONLY"} else 2
            output.write_text(json.dumps(result, default=str, ensure_ascii=False, indent=2) + "\n")
            return result, code
        if args.action in {"collect-data", "data-status", "import-poll"}:
            from sigbot.strategies.prc_sig_v1.acquisition import collect
            from sigbot.strategies.prc_sig_v1.data_inputs import import_poll, status

            output = spec.check_output(
                args.output
                or spec.report_directory + "/" + args.action.upper().replace("-", "_") + ".json"
            )
            if args.action == "collect-data":
                result = collect(
                    authorize=args.authorize_public_read,
                    per_source=args.per_source,
                    max_requests=args.max_requests,
                    seconds=args.seconds,
                    profile12_proxy=args.profile12_proxy,
                    profile12_port=args.profile12_port,
                )
            elif args.action == "data-status":
                result = status()
                result["requested_strategy"] = spec.strategy_id
            else:
                poll_store = (
                    Store(args.poll_database_url, "readonly") if args.poll_database_url else None
                )
                try:
                    result = import_poll(Path(args.input), store=poll_store)
                finally:
                    if poll_store:
                        poll_store.engine.dispose()
            output.write_text(json.dumps(result, default=str, ensure_ascii=False, indent=2) + "\n")
            code = (
                3 if args.action == "data-status" else 6 if result.get("status") == "PARTIAL" else 0
            )
            return result, code
        if args.action == "observe":
            from sigbot.strategies.ecr_sig_v1.observer import run_observer

            if args.shared_executor_config:
                if not args.review_input:
                    raise AuthorizationRequired("ECR_SHARED_REVIEW_INPUT_REQUIRED")
                if args.version == "1.1":
                    from sigbot.strategies.ecr_sig_v1.reference_runtime import ReferencePublisher

                    publisher = ReferencePublisher(args.shared_executor_config, args.review_input)
                else:
                    from sigbot.strategies.ecr_sig_v1.publisher import Publisher

                    publisher = Publisher(args.shared_executor_config, args.review_input)
                try:
                    return run_observer(
                        args,
                        cfg.contract_file,
                        on_started=publisher.started,
                        on_cycle=publisher.cycle,
                        on_prices=publisher.refresh_references if args.version == "1.1" else None,
                        fee_config=cfg,
                    ), 0
                finally:
                    publisher.close()
            return run_observer(args, cfg.contract_file, fee_config=cfg), 0
        if args.action == "sync-catalog":
            from sigbot.strategies.prc_sig_v1.catalog import sync as prc_catalog_sync

            output = spec.check_output(args.output or spec.report_directory + "/REAL_CATALOG.json")
            result = prc_catalog_sync(args, cfg.contract_file)
            output.write_text(json.dumps(result, default=str, ensure_ascii=False, indent=2) + "\n")
            return result, 0 if result["catalog_complete"] and result[
                "status"
            ] == "PASS_CATALOG_ONLY" else 2
        if cfg.mode.value != "OFFLINE" or cfg.business_network_enabled:
            raise AuthorizationRequired("ECR_LOCAL_STRATEGY_MODE_REQUIRED")
        if spec.strategy_id == "prc_sig":
            from sigbot.strategies.prc_sig_v1.replay import replay as strategy_replay

            scfg = PrcConfig.model_validate_json(
                Path(args.strategy_config or spec.config).read_text()
            )
            snapshot = PrcSnapshot.model_validate_json(Path(args.input).read_text())
        else:
            from sigbot.strategies.ecr_sig_v1.replay import replay as strategy_replay

            scfg = EcrConfig.model_validate_json(
                Path(args.strategy_config or spec.config).read_text()
            )
            snapshot = EcrSnapshot.model_validate_json(Path(args.input).read_text())
        if (scfg.strategy_id, scfg.strategy_version) != (
            spec.strategy_id,
            spec.version,
        ) or snapshot.account.initial != scfg.initial_capital:
            raise ValueError("strategy scope/initial capital mismatch")
        output = spec.check_output(
            args.output or spec.report_directory + "/" + args.action.upper() + ".json"
        )
        if args.action == "replay":
            result = strategy_replay(args.input)
        else:
            result = snapshot.plan()
        output.write_text(json.dumps(result, default=str, ensure_ascii=False, indent=2) + "\n")
        return result, 0 if result.get("status", "PASS") in {"PASS", "PASS_COMPUTATION_ONLY"} else 2
    if args.command == "doctor":
        return {
            "mode": cfg.mode.value,
            "layer": "STATIC",
            "real_writes_enabled": False,
            "credentials_loaded": False,
            "business_network_enabled": cfg.business_network_enabled,
            "database_integration_verified": False,
        }, 0
    if args.command == "contract":
        contract = Contract(cfg.contract_file)
        blockers = contract.report()
        return {
            "layer": "STATIC",
            "status": "BLOCKED" if any(blockers.values()) else "PASS",
            "capabilities": blockers,
            "authenticated_calls": 0,
        }, 2 if any(blockers.values()) else 0
    if args.command == "replay":
        result = replay(args.fixture)
        if args.output:
            Path(args.output).parent.mkdir(parents=True, exist_ok=True)
            Path(args.output).write_text(json.dumps(result, indent=2))
        return result, 0
    if args.command == "watchdog":
        result = watchdog(args.heartbeat, args.output, Clock())
        return result, 3 if result["action_required"] else 0
    if args.command == "orders" and args.action in {"submit", "cancel"}:
        raise AuthorizationRequired("LIVE_WRITES_DISABLED_IN_THIS_DELIVERY")
    if args.command in {"account", "reconcile", "settle"}:
        raise ContractNotVerified("REAL_READ_BLOCKED:OFFICIAL_TYPED_CONTRACT_UNVERIFIED")
    store = Store(args.database_url or cfg.database_url, cfg.namespace)
    try:
        if store.engine.dialect.name == "sqlite":
            store.create_for_mock()
        if args.command == "catalog":
            catalog = Catalog(store)
            if args.action == "import-seed":
                return catalog.import_seed(args.file), 0
            if args.action == "audit":
                return catalog.audit(args.output), 0
            if args.action == "sync":
                raise ContractNotVerified("CONTRACT_UNVERIFIED:market_list")
            from sigbot.domain import utc_time

            catalog.review(
                args.market,
                args.rules_hash,
                args.mapping_hash,
                args.operator,
                utc_time(args.expires_at),
            )
            return {"status": "REVIEW_RECORDED"}, 0
        if args.command == "orders":
            intent = Intent.model_validate_json(Path(args.intent_file).read_text())
            from sigbot.domain import utc_time
            from sigbot.strategies.router import StrategyRouter

            planner = StrategyRouter(
                store,
                cfg,
                Clock(),
                utc_time("2026-10-01T16:00:00Z"),
                utc_time("2026-11-04T17:00:00Z"),
            )
            from sigbot.live import bindings

            result = {
                "intent": intent.model_dump(mode="json"),
                "plan": asdict(planner.preview(intent)),
                "bindings": bindings(cfg),
                "submitted": False,
            }
            if args.output:
                from sigbot.domain import canonical

                Path(args.output).write_text(canonical(result))
            return result, 0
        controls = Controls(store, Clock())
        account_id = cfg.expected_account_id or "SIM-ACCOUNT"
        if args.command == "control":
            if args.action == "drain":
                raise AuthorizationRequired("NO_REMOTE_DRAIN_WITHOUT_VERIFIED_ADAPTER")
            controls.pause(account_id, args.reason)
        return controls.status(account_id), 0
    finally:
        store.engine.dispose()


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        cfg = Config.load(args.config)
        result, code = run(args, cfg)
    except SigbotError as exc:
        result, code = {"status": "BLOCKED", "code": exc.code, "real_writes": 0}, exc.exit_code
        if exc.execution_attempts is not None:
            result.pop("real_writes")
            result["write_attempts"] = exc.execution_attempts
            result["real_write_outcome"] = "SEE_PERSISTED_RECEIPTS_AND_OMS"
    except (ValidationError, ValueError, OSError):
        result, code = {"status": "FAIL", "code": "INVALID_CONFIG_OR_INPUT", "real_writes": 0}, 2
    except SQLAlchemyError:
        result, code = {"status": "FAIL", "code": "STORAGE_UNAVAILABLE", "real_writes": 0}, 6
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return code


if __name__ == "__main__":
    sys.exit(main())
