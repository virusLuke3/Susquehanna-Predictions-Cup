"""S3 proposals and holding recovery through the existing shared account OMS.

The user authorized live execution on 2026-10-14. Authorization and scientific
validation are separate: no Paper runner, probe trade or direct order client.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy import select, text
from sqlalchemy.exc import SQLAlchemyError

from sigbot.command_queue import CommandQueue
from sigbot.config import Config
from sigbot.domain import ZERO, Intent, content_hash, utc_time
from sigbot.errors import AuthorizationRequired, RiskRejected, SigbotError
from sigbot.readcheck import ReadCheck, load_key
from sigbot.risk import Planner
from sigbot.storage import (
    Account,
    FillRow,
    Instrument,
    IntentRow,
    Order,
    Position,
    PositionLot,
    Store,
    StrategyEvent,
)
from sigbot.strategies.semantic_sig_v1.core import number as money
from sigbot.strategies.semantic_sig_v1.registry import write_json

IDENTITY = ("semantic_sig", "1.0")


def cost_audit(config, args, root, remaining):
    """A bounded current L2/rule read, preserving the two own-order receipts."""
    reader = ReadCheck(
        load_key(args.env_file),
        tournament_slug=config.expected_tournament_slug,
        root=Path("runtime/readonly"),
        max_requests=max(1, remaining),
        seconds=args.seconds,
        contract_file=config.contract_file,
    )
    reader.reader.config.expected_account_id = config.expected_account_id
    reader.reader.config.expected_tournament_id = config.expected_tournament_id
    reader.reader.config.expected_tournament_slug = config.expected_tournament_slug
    costs: dict = {
        "fee_verified_for_semantic": False,
        "observations": [],
        "orders": 0,
        "cancels": 0,
    }
    try:
        before, _ = reader.check(
            "open_orders",
            lambda: reader.pages(
                "open_orders",
                query={
                    "tournamentId": config.expected_tournament_id,
                    "status": "open",
                    "limit": 200,
                },
            ),
        )
        targets = sorted(
            {
                r["target"]
                for r in json.loads(Path(args.model_validation).read_text()).get("relations", [])
            }
        )
        target = next(
            (
                eid
                for eid in targets
                if any(i.endswith("-" + eid + "-YES") for i in config.allowed_instruments)
            ),
            None,
        )
        if target:
            registry = json.loads(
                Path("reports/strategies/semantic_sig_v1/market_registry.json").read_text()
            )
            selected = next(m for m in registry["markets"] if m["exchange_id"] == target)
            mid = selected["sig_market_id"]
            detail = reader.call(
                "market_detail",
                path_params={"id": mid},
                query={"tournamentId": config.expected_tournament_id},
            )
            nodes = reader.call(
                "market_nodes",
                path_params={"id": mid},
                query={"tournamentId": config.expected_tournament_id},
            )
            book = reader.call(
                "orderbook",
                path_params={"id": target},
                query={"tournamentId": config.expected_tournament_id, "depth": 200},
            )
            costs["observations"].append(
                {
                    "market_id": mid,
                    "exchange_id": target,
                    "identity_matches": detail["id"] == book["marketId"] == mid
                    and book["exchangeId"] == target,
                    "current_predicate_sha256": content_hash(nodes),
                    "book_asof": book.get("asOf"),
                    "bid_levels": len(book["bids"]),
                    "ask_levels": len(book["asks"]),
                    "best_bid": str(book["bids"][0]["price"]) if book["bids"] else None,
                    "best_ask": str(book["asks"][0]["price"]) if book["asks"] else None,
                    "raw_book_sha256": reader.requests[-1]["response_sha256"],
                }
            )
        after, _ = reader.check(
            "open_orders",
            lambda: reader.pages(
                "open_orders",
                query={
                    "tournamentId": config.expected_tournament_id,
                    "status": "open",
                    "limit": 200,
                },
            ),
        )
        costs.update(
            own_orders_before=len(before),
            own_orders_after=len(after),
            own_order_lists_equal=content_hash(before) == content_hash(after),
            own_order_exclusion="STABLE_READ_WINDOW_ONLY_NO_ATOMIC_ACCOUNT_BOOK_WATERMARK",
        )
    finally:
        costs["receipt_manifest"] = reader.requests
        reader.reader.close()
        reader.key = ""
        write_json(root / "cost_contract_audit.json", costs)
    return costs


def model_gate(path, digest=None):
    raw = Path(path).read_bytes()
    if digest is not None and hashlib.sha256(raw).hexdigest() != digest:
        raise RiskRejected("SEMANTIC_MODEL_VERSION_CHANGED")
    model = json.loads(raw)
    if model.get("synthetic_data_used") is not False or model.get("live_eligible") is not True:
        raise RiskRejected("SEMANTIC_MODEL_NOT_VALIDATED_FOR_LIVE")
    manifest = Path(model["dataset_manifest_path"]).read_bytes()
    if hashlib.sha256(manifest).hexdigest() != model["dataset_manifest_sha256"]:
        raise RiskRejected("SEMANTIC_DATASET_BINDING_CHANGED")
    if content_hash(model["protocol"]) != model["protocol_hash"]:
        raise RiskRejected("SEMANTIC_PROTOCOL_CHANGED")
    dataset = json.loads(manifest)
    if (
        dataset.get("synthetic_data_used") is not False
        or dataset.get("executable_capacity_verified") is not True
        or dataset.get("historical_rules_pit") != "VERIFIED"
        or dataset.get("historical_own_order_exclusion") != "VERIFIED"
    ):
        raise RiskRejected("SEMANTIC_EXECUTABLE_HISTORY_NOT_VALIDATED")
    return model


class SemanticPlanner(Planner):
    """Common account gates remain in Planner; only semantic admission is added."""

    def envelope(self, intent):
        if (intent.strategy_id, intent.strategy_version) != IDENTITY:
            raise RiskRejected("SEMANTIC_STRATEGY_IDENTITY_MISMATCH")
        value = json.loads(intent.note or "{}")
        if value.get("kind") not in {"ENTRY", "EXIT"} or not value.get("event_key"):
            raise RiskRejected("SEMANTIC_SIGNAL_METADATA_REQUIRED")
        return value

    def event(self, session, account, event_key):
        return session.get(StrategyEvent, (account, *IDENTITY, event_key))

    def calculate(self, session, account, intent):
        plan = super().calculate(session, account, intent)
        self.check_semantic(session, account, intent, plan.price, plan.quantity)
        return plan

    def check_semantic(self, session, account, intent, price, quantity):
        note = self.envelope(intent)
        inst = session.get(Instrument, intent.instrument_id)
        if inst.outcome != "YES":
            raise RiskRejected("SEMANTIC_OUTCOME_NOT_VALIDATED")
        event = self.event(session, account.id, note["event_key"])
        if not inst.book.get("own_orders_known"):
            raise RiskRejected("SEMANTIC_EXTERNAL_DEPTH_NOT_VERIFIED")
        if note["event_key"] != inst.event_group:
            raise RiskRejected("SEMANTIC_OFFICIAL_EVENT_BINDING_CHANGED")
        if note["kind"] == "EXIT":
            if event is None or event.instrument_id != inst.id or event.state == "CLOSED":
                raise RiskRejected("SEMANTIC_EXIT_OWNERSHIP_REQUIRED")
            if intent.action != "REDUCE_TO_TARGET":
                raise RiskRejected("SEMANTIC_EXIT_MUST_REDUCE_ONLY")
            return  # Risk reduction does not require a still-profitable forecast.
        if intent.action != "INCREASE_TO_TARGET" or (event and event.state != "CLOSED"):
            raise RiskRejected("SEMANTIC_EVENT_ALREADY_OCCUPIED")
        if not intent.synthetic:
            model = model_gate(note["model_path"], note["model_sha256"])
            relation = next(
                (r for r in model["relations"] if r["edge_id"] == note["edge_id"]), None
            )
            if relation is None or relation.get("validated") is not True or relation.get("reasons"):
                raise RiskRejected("SEMANTIC_RELATION_NOT_VALIDATED")
            if model.get("strategy_fee_sha256") != note.get("fee_sha256"):
                raise RiskRejected("SEMANTIC_STRATEGY_FEE_BINDING_CHANGED")
        available = utc_time(note["feature_available_at"])
        if not 0 <= (self.clock.now - available).total_seconds() <= 5:
            raise RiskRejected("SEMANTIC_LIVE_FEATURES_STALE_OR_FUTURE")
        if not 0 < int(note["horizon_seconds"]) <= 300:
            raise RiskRejected("SEMANTIC_UNVALIDATED_HOLDING_HORIZON")
        if (
            money(note["forecast_exit_bid"])
            - price
            - 2 * inst.fee_per_share
            - money(note["uncertainty_buffer"])
            - money(note["risk_buffer"])
        ) <= 0:
            raise RiskRejected("SEMANTIC_CURRENT_COST_ADJUSTED_EDGE_NOT_POSITIVE")
        if quantity <= 0:
            return
        # Share the existing account/event limits, including every other strategy.
        if int(note.get("max_holding_events", 1)) != 1:
            raise RiskRejected("SEMANTIC_INITIAL_SINGLE_EVENT_SCOPE_REQUIRED")
        active = session.scalar(
            select(StrategyEvent).where(
                StrategyEvent.account_id == account.id,
                StrategyEvent.strategy_id == IDENTITY[0],
                StrategyEvent.strategy_version == IDENTITY[1],
                StrategyEvent.state != "CLOSED",
            )
        )
        if active is not None:
            raise RiskRejected("SEMANTIC_SINGLE_EVENT_ALREADY_ACTIVE")

    def check_dispatch(self, session, account, intent, order):
        # The ordinary Executor refreshed/rechecked account, rule, price and
        # reserve. Recheck the frozen signal without claiming the already-owned
        # reservation is a second entry.
        note = self.envelope(intent)
        if note["kind"] == "ENTRY":
            if not intent.synthetic:
                model_gate(note["model_path"], note["model_sha256"])
            if (
                not 0
                <= (self.clock.now - utc_time(note["feature_available_at"])).total_seconds()
                <= 5
            ):
                raise RiskRejected("SEMANTIC_SIGNAL_EXPIRED_BEFORE_SEND")
            inst = session.get(Instrument, order.instrument_id)
            if (
                not inst.book.get("own_orders_known")
                or money(note["forecast_exit_bid"])
                - order.price
                - 2 * inst.fee_per_share
                - money(note["uncertainty_buffer"])
                - money(note["risk_buffer"])
                <= 0
            ):
                raise RiskRejected("SEMANTIC_EDGE_DISAPPEARED_BEFORE_SEND")

    def on_reserved(self, session, account, intent, plan, order_id):
        note = self.envelope(intent)
        event = self.event(session, account.id, note["event_key"])
        if note["kind"] == "ENTRY":
            inst = session.get(Instrument, intent.instrument_id)
            if event is None:
                event = StrategyEvent(
                    account_id=account.id,
                    strategy_id=IDENTITY[0],
                    strategy_version=IDENTITY[1],
                    event_key=note["event_key"],
                    instrument_id=inst.id,
                    state="RESERVED",
                    state_group=note.get("state", "UNKNOWN"),
                    shock_group=note["edge_id"],
                    direction_bucket=inst.outcome,
                    future_fee_reserve=ZERO,
                    data={},
                )
                session.add(event)
            event.holding_started_at = None
            event.state, event.data = (
                "RESERVED",
                {
                    **note,
                    "entry_reserved_at": self.clock.now.isoformat(),
                    "entry_order_id": order_id,
                },
            )
            event.future_fee_reserve = plan.quantity * inst.fee_per_share
        else:
            event.state = "EXIT_PENDING"

    def on_observed(self, session, account, order, observation):
        intent = Intent.model_validate(session.get(IntentRow, order.intent_id).payload)
        note = self.envelope(intent)
        event = self.event(session, account.id, note["event_key"])
        position = session.get(Position, (account.id, order.instrument_id))
        quantity = position.quantity if position else ZERO
        pending = sum(
            (
                o.quantity - o.filled
                for o in session.scalars(
                    select(Order).where(
                        Order.account_id == account.id,
                        Order.instrument_id == order.instrument_id,
                        Order.side == "BUY",
                    )
                )
                if o.reserve_cash
            ),
            ZERO,
        )
        inst = session.get(Instrument, order.instrument_id)
        event.future_fee_reserve = (quantity + pending) * inst.fee_per_share
        if quantity:
            times = [utc_time(f.filled_at) for f in observation.fills if f.filled_at is not None]
            if times and min(times) > self.clock.now:
                raise RiskRejected("SEMANTIC_FUTURE_FILL_TIME")
            if event.holding_started_at is None:
                event.holding_started_at = (
                    min(times) if times else utc_time(event.data["entry_reserved_at"])
                )
            event.state = "HOLDING" if order.side == "BUY" else "EXIT_PENDING"
        elif pending:
            event.state = "RESERVED"
        else:
            event.state, event.future_fee_reserve = "CLOSED", ZERO
            event.last_exit_at = self.clock.now

    def on_unsent_expired(self, session, account, intent, order):
        note = self.envelope(intent)
        event = self.event(session, account.id, note["event_key"])
        if event:
            event.state = "CLOSED" if note["kind"] == "ENTRY" else "HOLDING"
            if note["kind"] == "ENTRY":
                event.future_fee_reserve = ZERO

    on_unsent_canceled = on_unsent_expired

    def exit_intent(self, account_id, event_key):
        with self.store.transaction() as session:
            account = self.store.lock_account(session, account_id)
            event = self.event(session, account_id, event_key)
            if event is None or event.state == "CLOSED":
                return None
            if event.holding_started_at is None or self.clock.now < utc_time(
                event.data["feature_available_at"]
            ) + timedelta(seconds=event.data["horizon_seconds"]):
                return None
            inst = session.get(Instrument, event.instrument_id)
            if inst is None or inst.fee_per_share is None:
                raise RiskRejected("SEMANTIC_EXIT_INSTRUMENT_OR_FEE_UNAVAILABLE")
            position = session.get(Position, (account_id, inst.id))
            if position is None or position.quantity <= 0:
                return None
            lots = list(
                session.scalars(
                    select(PositionLot).where(
                        PositionLot.account_id == account_id, PositionLot.instrument_id == inst.id
                    )
                )
            )
            if sum((lot.quantity for lot in lots), ZERO) != position.quantity:
                raise RiskRejected("SEMANTIC_INVENTORY_PROVENANCE_UNKNOWN")
            for lot in lots:
                if not lot.quantity:
                    continue
                fill = session.get(FillRow, (account_id, lot.economic_id.removeprefix("fill:")))
                order = session.get(Order, fill.order_id) if fill else None
                owner = session.get(IntentRow, order.intent_id) if order else None
                if (
                    owner is None
                    or (owner.payload.get("strategy_id"), owner.payload.get("strategy_version"))
                    != IDENTITY
                ):
                    raise RiskRejected("SEMANTIC_FOREIGN_INVENTORY_PROTECTED")
            orders = list(
                session.scalars(
                    select(Order).where(
                        Order.account_id == account.id, Order.instrument_id == inst.id
                    )
                )
            )
            if any(
                o.reserve_cash
                or o.reserve_quantity
                or o.request_state in {"UNKNOWN", "SENDING"}
                or o.cancel_state in {"CANCEL_REQUESTED", "CANCEL_UNKNOWN"}
                for o in orders
            ):
                return None
            levels = [
                (money(x["price"]), money(x["quantity"]))
                for x in inst.book.get("bids", [])
                if not x.get("own")
            ]
            if not inst.book.get("own_orders_known") or not levels:
                raise RiskRejected("SEMANTIC_EXIT_DEPTH_UNAVAILABLE")
            price = max(p for p, q in levels if q > 0)
            quantity = min(position.quantity, sum((q for p, q in levels if p == price), ZERO))
            quantity = (quantity // inst.step) * inst.step
            if quantity <= 0:
                raise RiskRejected("SEMANTIC_EXIT_NO_EXECUTABLE_QUANTITY")
            return Intent.model_validate(
                {
                    "schema_version": "1.0",
                    "intent_id": (
                        "SIM-SEMANTIC-EXIT-"
                        if account.namespace == "offline"
                        else "SIG-SEMANTIC-EXIT-"
                    )
                    + content_hash([account.id, event_key, position.quantity, self.clock.now]),
                    "synthetic": account.namespace == "offline",
                    "account_id": account.id,
                    "tournament_id": account.tournament,
                    "instrument_id": inst.id,
                    "outcome_side": inst.outcome,
                    "action": "REDUCE_TO_TARGET",
                    "target_quantity": position.quantity - quantity,
                    "limit_price": price,
                    "max_order_notional": quantity * (price + inst.fee_per_share),
                    "created_at": self.clock.now,
                    "expires_at": self.clock.now + timedelta(seconds=5),
                    "rules_hash": inst.rules_hash,
                    "mapping_hash": inst.mapping_hash,
                    "strategy_id": IDENTITY[0],
                    "strategy_version": IDENTITY[1],
                    "note": json.dumps(
                        {"kind": "EXIT", "event_key": event_key, "reason": "HORIZON_EXIT"}
                    ),
                }
            )


def preflight(args):
    if not args.authorize_read:
        raise AuthorizationRequired("SEMANTIC_EXPLICIT_READ_REQUIRED")
    root = Path(args.output)
    root.mkdir(parents=True, exist_ok=True)
    report: dict = {
        "at": datetime.now(timezone.utc).isoformat(),
        "layer": "REAL_READ_LIVE_PREFLIGHT",
        "requested_mode": "LIVE_LIMITED",
        "paper_requested": False,
        "user_live_authorization": "EXPLICIT_USER_MESSAGE_2026_10_14",
        "real_submit_attempts": 0,
        "real_cancel_attempts": 0,
        "checks": {},
        "blockers": [],
    }
    reads, store = None, None
    try:
        settings = json.loads(Path(args.gateway_settings).read_text())
        base = settings["strategies"][0]
        config = Config.load(base["application_config"])
        if not config.expected_account_id or not config.expected_tournament_slug:
            raise AuthorizationRequired("SEMANTIC_REAL_ACCOUNT_SCOPE_REQUIRED")
        approval = json.loads(Path(base["approval_file"]).read_text())
        report["checks"]["existing_operator_eligibility"] = (
            approval.get("eligibility_confirmed_by_operator") is True
        )
        report["checks"]["shared_semantic_authority_registered"] = any(
            (s["strategy_id"], s["strategy_version"]) == IDENTITY for s in settings["strategies"]
        )
        report["account_sha256"] = hashlib.sha256(config.expected_account_id.encode()).hexdigest()
        report["tournament_id"] = config.expected_tournament_id
        report["existing_account_risk_limits"] = config.risk.model_dump(mode="json")
        report["existing_shared_request_limits"] = {
            k: getattr(config, k) for k in ["read_budget", "write_budget", "new_order_budget"]
        }
        report["checks"]["research_model_validated"] = False
        try:
            model_gate(args.model_validation)
            report["checks"]["research_model_validated"] = True
        except (RiskRejected, OSError, ValueError) as exc:
            report["blockers"].append(
                exc.code if isinstance(exc, SigbotError) else "SEMANTIC_MODEL_ARTIFACT_UNAVAILABLE"
            )
        fee = json.loads(Path(config.live_fee_file).read_text()) if config.live_fee_file else {}
        report["cost_contract"] = {
            "kind": fee.get("kind"),
            "verified": fee.get("verified"),
            "scope": "EXISTING_STRATEGY_NOT_AUTOMATIC_SEMANTIC_APPROVAL",
        }
        report["checks"]["semantic_fee_contract_approved"] = False
        for item in settings["strategies"]:
            if (item["strategy_id"], item["strategy_version"]) == IDENTITY:
                semantic_config = Config.load(item["application_config"])
                semantic_approval = json.loads(Path(item["approval_file"]).read_text())
                report["checks"]["semantic_fee_contract_approved"] = bool(
                    semantic_config.live_fee_file
                    and (
                        fee.get("verified") is True
                        or semantic_approval.get("accept_empirical_fee_model") is True
                    )
                )
        from sigbot.strategies.semantic_sig_v1.research import CONTRACT, reference_check

        report["official_reference"] = reference_check(CONTRACT, root / "reference")
        reads = ReadCheck(
            load_key(args.env_file),
            tournament_slug=config.expected_tournament_slug,
            root=Path("runtime/readonly"),
            max_requests=args.max_requests,
            seconds=args.seconds,
            contract_file=CONTRACT,
        )
        account = reads.call("account_identity")
        report["checks"]["official_account_matches"] = account["id"] == config.expected_account_id
        reads.reader.config.expected_account_id = config.expected_account_id
        reads.reader.config.expected_tournament_id = config.expected_tournament_id
        reads.reader.config.expected_tournament_slug = config.expected_tournament_slug
        cup = reads.call(
            "tournament_identity", path_params={"slug": config.expected_tournament_slug}
        )
        now = datetime.now(timezone.utc)
        report["checks"]["official_tournament_matches"] = cup["id"] == config.expected_tournament_id
        report["checks"]["official_enrolment_complete"] = not cup["isPendingEnrolment"]
        report["checks"]["within_trading_period"] = (
            utc_time(cup["startDate"])
            <= now
            < utc_time(cup["endDate"]) - timedelta(seconds=config.drain_seconds)
        )
        report["trading_period"] = {"start": cup["startDate"], "end": cup["endDate"]}
        if args.audit_costs:
            report["cost_read_audit"] = cost_audit(
                config, args, root, args.max_requests - len(reads.requests)
            )
        store = Store(config.database_url, "live")
        with store.transaction() as session:
            session.execute(text("SET TRANSACTION READ ONLY"))
            local = session.get(Account, config.expected_account_id)
            orders = list(
                session.scalars(select(Order).where(Order.account_id == config.expected_account_id))
            )
            unresolved = [
                o
                for o in orders
                if o.request_state in {"UNKNOWN", "SENDING"}
                or o.cancel_state in {"CANCEL_REQUESTED", "CANCEL_UNKNOWN"}
            ]
            report["unknown_orders"] = len(unresolved)
            report["checks"]["no_unknown_account_orders"] = not unresolved
            report["checks"]["shared_account_healthy_reconciled"] = bool(
                local
                and local.lifecycle == "HEALTHY"
                and local.reconciled_at
                and 0
                <= (now - utc_time(local.reconciled_at)).total_seconds()
                <= config.account_ttl_seconds
            )
            report["approved_instrument_versions"] = [
                {
                    "instrument_id": iid,
                    "rules_hash": inst.rules_hash,
                    "mapping_hash": inst.mapping_hash,
                    "reviewed": inst.reviewed,
                    "review_until": inst.review_until,
                    "tick": str(inst.tick),
                    "step": str(inst.step),
                }
                for iid in config.allowed_instruments
                if (inst := session.get(Instrument, iid)) is not None
            ]
        envelope = json.loads(Path(args.shared_source).read_text())
        payload, state = envelope["payload"], envelope["payload"]["feed_state"]
        now = datetime.now(timezone.utc)
        report["checks"]["shared_source_identity_integrity"] = (
            envelope["digest"] == content_hash(payload)
            and payload["account_id"] == config.expected_account_id
            and payload["tournament_id"] == config.expected_tournament_id
        )
        checked = state.get("connection_checked_at")
        report["checks"]["shared_connection_confirmed"] = bool(
            checked
            and state["subscription_status"] == "SUBSCRIBED"
            and 0 <= (now - utc_time(checked)).total_seconds() <= 5
            and 0 <= (now - utc_time(payload["published_at"])).total_seconds() <= 5
        )
        report["shared_source_pending_markets"] = len(state["pending_reasons"])
        validated = json.loads(Path(args.model_validation).read_text())
        selected = {
            eid
            for relation in validated.get("relations", [])
            if relation.get("validated") is True
            for eid in (relation["source"], relation["target"])
        }
        exchange_markets = {eid: mid for mid, eids in payload["mapping"].items() for eid in eids}
        report["checks"]["selected_source_target_continuity"] = bool(selected) and all(
            eid in exchange_markets
            and exchange_markets[eid] not in state["pending_reasons"]
            and eid in payload["books"]
            for eid in selected
        )
    except (SigbotError, SQLAlchemyError, OSError, ValueError, KeyError, TypeError) as exc:
        report["blockers"].append(
            exc.code
            if isinstance(exc, SigbotError)
            else "SEMANTIC_PREFLIGHT_CONFIG_STORAGE_OR_SCHEMA_FAILURE"
        )
    finally:
        if reads:
            report["authenticated_read_requests"] = reads.requests
            reads.reader.close()
            reads.key = ""
        if store:
            store.engine.dispose()
    report["blockers"].extend(name for name, passed in report["checks"].items() if not passed)
    report["status"] = "READY" if not report["blockers"] else "BLOCKED"
    write_json(root / "live_preflight.json", report)
    return report


def publish_signal(store, config, signal, *, authorized=False):
    if not authorized or store.namespace != "live" or not config.live_trading_enabled:
        raise AuthorizationRequired("SEMANTIC_EXPLICIT_LIVE_SUBMIT_REQUIRED")
    model_gate(signal["model_path"], signal["model_sha256"])
    intent = Intent.model_validate(signal["intent"])
    if intent.synthetic or (intent.strategy_id, intent.strategy_version) != IDENTITY:
        raise AuthorizationRequired("SEMANTIC_REAL_STRATEGY_SCOPE_REQUIRED")
    with store.transaction() as session:
        event = session.get(
            StrategyEvent,
            (intent.account_id, *IDENTITY, json.loads(intent.note or "{}")["event_key"]),
        )
        if event and event.state != "CLOSED":
            raise RiskRejected("SEMANTIC_EVENT_ALREADY_ACTIVE")
    return CommandQueue(store, config.expected_account_id, *IDENTITY).publish(intent)


def run_live(args):
    report = preflight(args)
    if args.action == "live-preflight" or report["status"] != "READY":
        return {
            "status": report["status"],
            "blockers": report["blockers"],
            "real_submit_attempts": 0,
            "paper_started": False,
        }, 2 if report["status"] != "READY" else 0
    if not args.authorize_submit or not args.signal_file:
        raise AuthorizationRequired("SEMANTIC_EXPLICIT_LIVE_SIGNAL_AND_AUTHORIZATION_REQUIRED")
    settings = json.loads(Path(args.gateway_settings).read_text())
    item = next(
        s for s in settings["strategies"] if (s["strategy_id"], s["strategy_version"]) == IDENTITY
    )
    config = Config.load(item["application_config"])
    store = Store(config.database_url, "live")
    try:
        command = publish_signal(
            store, config, json.loads(Path(args.signal_file).read_text()), authorized=True
        )
        result = {
            "status": "QUEUED_TO_EXISTING_ACCOUNT_OMS",
            "command_id": command,
            "real_submit_attempts_by_producer": 0,
            "order_receipt_status": "NOT_YET_OBSERVED",
            "paper_started": False,
        }
        write_json(Path(args.output) / "live_execution.json", result)
        return result, 0
    finally:
        store.engine.dispose()
