"""ECR 1.1 producer: current verified books -> existing shared command queue."""

import json
from collections import Counter
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from sqlalchemy import select

from sigbot.command_queue import CommandQueue
from sigbot.config import Config
from sigbot.domain import ZERO, Clock, Intent, content_hash, utc_time
from sigbot.errors import AuthorizationRequired, SigbotError
from sigbot.storage import Account, ExecutionCommand, Instrument, Market, Store
from sigbot.storage_policy import bounded_storage
from sigbot.strategies.ecr_sig_v1 import core
from sigbot.strategies.ecr_sig_v1.observer import append_json, atomic_json, sig_book
from sigbot.strategies.ecr_sig_v1.oms import proposal_from_json
from sigbot.strategies.ecr_sig_v1.publicdata import PublicReads, monitoring_matches, public_book
from sigbot.strategies.ecr_sig_v1.reference import (
    IDENTITY,
    REFERENCE_FILE,
    Parameters,
    ReferencePlanner,
    book,
    signal_intent,
)


def refresh_execution_reference(store, config, instrument_id):
    """One selected public book through the existing reader, before allocation.

    This is an ephemeral read cache. Orders, reservations and fills remain in
    the common durable OMS; restarting never recreates a financial operation.
    """
    frame = json.loads(REFERENCE_FILE.read_text())
    if (frame.get("account_id"), frame.get("tournament_id")) != (
        config.expected_account_id, config.expected_tournament_id,
    ):
        raise AuthorizationRequired("ECR11_REFERENCE_ACCOUNT_MISMATCH")
    previous = frame.get("books_by_instrument", {}).get(instrument_id)
    if not previous or not frame.get("public_proxy"):
        raise AuthorizationRequired("ECR11_REFERENCE_SOURCE_NOT_AVAILABLE")
    public = PublicReads(
        Path("runtime/readonly/strategies/ecr_sig_v1/execution-reference-reads"),
        proxy=frame["public_proxy"],
    )
    token = previous["external_contract_id"]
    raw = public.books([token])
    if not isinstance(raw, list) or len(raw) != 1:
        raise AuthorizationRequired("ECR11_SINGLE_BOOK_RESPONSE_MISMATCH")
    contract = {
        "operator": "POLYMARKET", "contract_id": token,
        "condition_id": previous["condition_id"], "outcome_side": "YES",
    }
    current = {**previous, "book": public_book(raw[0], contract, public.last_receipt)}
    cache = getattr(store, "ecr_reference_books", {})
    cache[instrument_id] = current
    store.ecr_reference_books = cache
    return current


class ReferencePublisher:
    def __init__(self, settings_file, parameter_file, *, publish=True, capture_path=None):
        document = json.loads(Path(settings_file).read_text())
        scope = next(
            s
            for s in document["strategies"]
            if (s["strategy_id"], s["strategy_version"]) == IDENTITY
        )
        self.config = Config.load(scope["application_config"])
        self.parameters = Parameters.model_validate_json(Path(parameter_file).read_text())
        self.store = Store(self.config.database_url, "live")
        self.clock = Clock(realtime=True)
        self.queue = CommandQueue(self.store, self.config.expected_account_id, *IDENTITY)
        self.policy = None
        self.enabled = publish
        self.queued = 0
        self.capture_path = Path(capture_path) if capture_path else None
        if self.capture_path is None and self.parameters.economic_revision == "joint_v2":
            self.capture_path = Path("runtime/readonly/ecr-joint-25000-forward/observations.jsonl")
        if bounded_storage():
            self.capture_path = None
        if self.capture_path is not None:
            self.capture_path.parent.mkdir(parents=True, exist_ok=True)
        self.next_capture = datetime.min.replace(tzinfo=timezone.utc)

    def started(self, observer):
        if (observer.tid, observer.reads.reader.config.expected_account_id) != (
            self.config.expected_tournament_id,
            self.config.expected_account_id,
        ):
            raise AuthorizationRequired("ECR11_OBSERVER_ACCOUNT_MISMATCH")
        self.policy = ReferencePlanner(
            self.store,
            self.config,
            self.clock,
            utc_time(observer.cup["startDate"]),
            utc_time(observer.cup["endDate"]),
        )
        with self.store.transaction() as session:
            self.targets = [
                (i.id, i.exchange_id, m.api_id, i.event_group)
                for i, m in session.execute(
                    select(Instrument, Market).join(Market, Instrument.market_id == Market.id).where(
                        Instrument.tournament == self.config.expected_tournament_id,
                        Instrument.outcome == "YES",
                    )
                )
                if i.id in self.config.allowed_instruments
            ]
        observer.state.update(
            execution_role="PRODUCER_ONLY",
            mode="PRODUCER_ONLY",
            normal_live_bridge_enabled=True,
            strategy_version="1.1",
            shared_executor="sigbot-sig-account-gateway.service",
        )

    def refresh_references(self, observer):
        if not hasattr(self, "targets"):
            return {}
        _, external, _ = observer.collector.snapshot()
        rows = {r["api_market_id"]: r for r in observer.rows}
        references = {}
        for iid, eid, mid, _event in self.targets:
            row = rows.get(mid)
            if row is None:
                continue
            choices = monitoring_matches(
                row, observer.external, market_id=observer.price_market_ids.get(mid)
            )
            choices = [c for c in choices if c["operator"] == "POLYMARKET"]
            if len(choices) != 1 or choices[0]["geography"][1] == "CHAMBER_CONTROL":
                continue
            remote = choices[0]
            current = external.get(("POLYMARKET", remote["contract_id"]))
            if current is None:
                continue
            reference = {
                "instrument_id": iid,
                "exchange_id": eid,
                "external_market_id": remote["market_id"],
                "external_contract_id": remote["contract_id"],
                "condition_id": remote["condition_id"],
                "operator": "POLYMARKET",
                "outcome_side": "YES",
                "office": remote["geography"][1],
                "state": remote["geography"][0],
                "party": remote["party"],
                "year": 2026,
                "identity_verified": True,
                "settlement_equivalent": "NOT_REQUIRED_FOR_PRICE_REFERENCE",
                "book": current,
            }
            reference["binding"] = content_hash({k: v for k, v in reference.items() if k != "book"})
            references[iid] = reference
        atomic_json(
            REFERENCE_FILE,
            {
                "at": datetime.now(timezone.utc).isoformat(),
                "account_id": self.config.expected_account_id,
                "tournament_id": self.config.expected_tournament_id,
                "public_proxy": getattr(getattr(observer, "public", None), "proxy", None),
                "references": {
                    event: str(references[iid]["book"]["bid"])
                    for iid, _, _, event in self.targets
                    if iid in references and references[iid]["book"]["bid"] is not None
                },
                "books_by_instrument": references,
            },
        )
        return references

    def cycle(self, observer):
        assert self.policy is not None
        self.clock.sync()
        now = self.clock.now
        references = self.refresh_references(observer)
        observed = {
            r["sig_market_id"]: r
            for r in observer.state.get("price_monitoring", {}).get("records", [])
        }
        rejects: Counter[str] = Counter()
        proposals, captured = [], []
        capture_now = self.capture_path is not None and now >= self.next_capture
        capture_local = observer.collector.snapshot()[0] if capture_now else {}
        capture_rows = {row["api_market_id"]: row for row in observer.rows} if capture_now else {}
        model_status = "LEGACY"
        if self.parameters.economic_revision == "joint_v2":
            from sigbot.strategies.ecr_sig_v1 import joint

            try:
                model = joint.load_model(self.parameters, int(now.timestamp()))
                supported = sum(
                    row["clusters"] >= self.parameters.model_min_clusters and row["events"] >= 4
                    and Decimal(row["complete_fraction"]) >= Decimal(".8")
                    for horizons in model["groups"].values() for row in horizons.values()
                )
                model_status = "SUPPORTED" if supported else "CALIBRATION_PENDING"
            except (core.Reject, FileNotFoundError) as exc:
                model_status = str(exc)
        with self.store.transaction() as session:
            account = session.get(Account, self.config.expected_account_id)
            if account is None:
                raise AuthorizationRequired("ECR11_SHARED_ACCOUNT_MISSING")
            pending = set(
                session.scalars(
                    select(ExecutionCommand.instrument_id).where(
                        ExecutionCommand.account_id == account.id,
                        ExecutionCommand.strategy_id == IDENTITY[0],
                        ExecutionCommand.strategy_version == IDENTITY[1],
                        ExecutionCommand.expires_at > now,
                        ExecutionCommand.state.in_(["WAITING_ALLOCATION", "OMS_PENDING"]),
                    )
                )
            )
            active_instruments = {
                o.instrument_id
                for o, _ in self.policy.own_orders(session, account.id, active=True)
            }
            held_instruments = {
                lot.instrument_id for lot, _ in self.policy.owned_lots(session, account.id)
            }
            for iid, eid, mid, _ in self.targets:
                reference = references.get(iid)
                if reference is None:
                    rejects["REFERENCE_BOOK_MISSING_THIS_ROUND"] += 1
                    continue
                inst = session.get(Instrument, iid)
                if inst is None:
                    raise AuthorizationRequired("ECR11_INSTRUMENT_DISAPPEARED")
                if capture_now:
                    actual = capture_local.get(eid)
                    local_book, local_quality = {}, "GAP"
                    if actual is not None:
                        received = utc_time(actual.get("validation_received_at") or actual["received_at"])
                        requested = utc_time(actual.get("validation_requested_at") or actual["received_at"])
                        local_book = sig_book(actual, capture_rows[mid], received, requested)
                        local_quality = "HEALTHY" if local_book["fresh"] else "GAP"
                    captured.append(
                        {
                            "sig_market_id": mid,
                            "reference": reference,
                            "instrument": {
                                "id": iid,
                                "exchange_id": eid,
                                "outcome": inst.outcome,
                                "reviewed": inst.reviewed,
                                "fee_per_share": inst.fee_per_share,
                                "book": local_book,
                                "book_at": local_book.get("request_started_at"),
                                "quality": local_quality,
                                "event_group": inst.event_group,
                                "tick": inst.tick,
                                "step": inst.step,
                            },
                        }
                    )
                if iid in pending or iid in active_instruments:
                    rejects["PROPOSAL_OR_ORDER_ALREADY_PENDING"] += 1
                    continue
                if iid in held_instruments:
                    continue
                if model_status not in {"LEGACY", "SUPPORTED"}:
                    rejects[model_status] += 1
                    continue
                try:
                    intent = signal_intent(
                        account.id,
                        account.tournament,
                        inst,
                        reference,
                        observed.get(mid, {}).get("sig", {}),
                        self.parameters,
                        now,
                    )
                    score = Decimal(str(reference["book"]["bid"])) - intent.limit_price
                    proposals.append((score, intent))
                except (core.Reject, SigbotError) as exc:
                    rejects[exc.code if isinstance(exc, SigbotError) else str(exc)] += 1
        for _, intent in sorted(proposals, key=lambda item: item[0], reverse=True):
            if self.enabled:
                self.queue.publish(intent)
                self.queued += 1
        if capture_now:
            assert self.capture_path is not None
            append_json(self.capture_path, {"at": self.clock.sync().isoformat(), "observations": captured})
            self.next_capture = now + timedelta(seconds=60)
        exits = self.manage_orders_and_exits(
            references, now, {iid: observed.get(mid, {}).get("sig", {}) for iid, _, mid, _ in self.targets}
        ) if self.enabled else {}
        report = {
            "at": now.isoformat(),
            "strategy_id": IDENTITY[0],
            "strategy_version": IDENTITY[1],
            "status": "RUNNING_REFERENCE_PRICE_EXECUTION"
            if self.enabled
            else "LOCAL_PREVIEW_NO_COMMANDS",
            "reference_candidates": len(references),
            "price_signal_candidates": len(proposals),
            "queued_signal_proposals_total": self.queued,
            "rejections": dict(rejects),
            "exits": exits,
            "execution_owner": "SHARED_ACCOUNT_EXECUTOR",
            "producer_order_http_calls": 0,
            "economic_revision": self.parameters.economic_revision,
            "strategy_capital": str(self.parameters.strategy_cash),
            "model_status": model_status,
            "calibration_recording": str(self.capture_path) if self.capture_path else None,
        }
        observer.state["shared_execution"] = report
        observer.state.update(
            lifecycle=report["status"],
            signal_engine_status="CURRENT_PRICE_GAP_V11_SHARED_GATEWAY",
            qualified_signals=len(proposals),
        )
        atomic_json(
            Path("runtime/live/strategies/ecr_sig_v1/reference-execution-status.json"), report
        )

    def manage_orders_and_exits(self, references, now, observed=None):
        assert self.policy is not None
        cancellations, exits = [], []
        with self.store.transaction() as session:
            account = session.get(Account, self.config.expected_account_id)
            if account is None:
                raise AuthorizationRequired("ECR11_SHARED_ACCOUNT_MISSING")
            active = self.policy.own_orders(session, account.id, active=True)
            for order, _origin in active:
                if (
                    (now - utc_time(_origin.payload["created_at"])).total_seconds()
                    >= self.parameters.order_seconds
                ):
                    cancellations.append(order.id)
            instrument_ids = {
                lot.instrument_id for lot, _ in self.policy.owned_lots(session, account.id)
            }
            for iid in instrument_ids:
                inst = session.get(Instrument, iid)
                if inst is None:
                    raise AuthorizationRequired("ECR11_OWNED_INSTRUMENT_MISSING")
                event = self.policy.event(session, account.id, inst.exchange_id)
                lots = self.policy.owned_lots(session, account.id, iid)
                if not event or not lots or event.holding_started_at is None:
                    continue
                own_active = [o for o, _ in active if o.instrument_id == iid]
                if any(o.side == "SELL" for o in own_active):
                    continue
                first, frozen = lots[0]
                current = references.get(iid)
                missing = 0
                fresh = bool(
                    current
                    and current["book"].get("bid") is not None
                    and 0 <= (now - utc_time(current["book"]["request_started_at"])).total_seconds() <= 5
                )
                if fresh:
                    event.data = {**event.data, "reference_missing_since": None}
                else:
                    since = event.data.get("reference_missing_since") or now.isoformat()
                    event.data = {**event.data, "reference_missing_since": since}
                    missing = int((now - utc_time(since)).total_seconds())
                current_reference = None
                if (
                    fresh
                ):
                    current_reference = (
                        Decimal(str(current["book"]["bid"]))
                        if current["book"]["bid"] is not None
                        else None
                    )
                raw_plan = frozen["entry_plan"]
                plan = proposal_from_json(raw_plan)
                holding_parameters = Parameters.model_validate(frozen["parameters"])
                task = event.data.get("exit_task")
                if task and task.get("state") != "ACTIVE":
                    task = None
                full_book = None
                try:
                    current_book = book(
                        inst.book,
                        inst.book.get("validated_at") or inst.book_at,
                        verified=inst.quality == "HEALTHY",
                    )
                    current_book.check(int(now.timestamp()))
                    net_exit = core.consume(current_book.bids, first.quantity, False)[0]
                    bid = current_book.bid
                    full_book = current_book
                except (core.Reject, TypeError, ValueError):
                    quote = (observed or {}).get(iid, {})
                    if (
                        quote.get("status") != "FRESH" or quote.get("bid") is None
                        or not 0 <= (now - utc_time(quote["received_at"])).total_seconds() <= 30
                    ):
                        continue
                    # A current price can trigger an exit proposal. The gateway
                    # must verify actual size/depth and this condition before reservation.
                    bid = net_exit = Decimal(quote["bid"])
                if task:
                    reason = task["reason"]
                elif event.data.get("risk_exit") in self.policy.risk_reasons:
                    reason = event.data["risk_exit"]
                elif holding_parameters.economic_revision == "joint_v2":
                    from sigbot.strategies.ecr_sig_v1 import joint
                    from sigbot.strategies.ecr_sig_v1.reference import candidate

                    opened = int(utc_time(event.holding_started_at).timestamp())
                    reason = (
                        "STOP_TRIGGER" if net_exit <= plan.entry_cost_bound_per_share - plan.stop_trigger_per_share
                        else "TIME_EXIT" if int(now.timestamp()) >= opened + holding_parameters.holding_seconds
                        else "REFERENCE_LOST" if missing >= 120 else None
                    )
                    if reason is None and fresh and full_book is not None:
                        try:
                            reason, estimate = joint.exit_reason(
                                candidate(inst, current, now), first.quantity, plan, opened,
                                int(now.timestamp()), holding_parameters,
                                joint.load_model(holding_parameters, int(now.timestamp())),
                            )
                            event.data = {**event.data, "continuation_estimate": json.loads(json.dumps(estimate, default=str))}
                        except core.Reject as exc:
                            event.data = {**event.data, "continuation_unavailable": str(exc)}
                else:
                    reason = core.exit_reason(
                        plan, net_exit, current_reference, int(now.timestamp()),
                        reference_missing_seconds=missing,
                        holding_started_at=int(utc_time(event.holding_started_at).timestamp()),
                    )
                if reason is None or reason == "EXIT_BLOCKED":
                    continue
                if task is None:
                    task = {
                        "id": content_hash([account.id, iid, event.holding_started_at, frozen.get("entry_plan")]),
                        "state": "ACTIVE", "reason": reason,
                        "confirmed_at": now.isoformat(), "target_quantity": "0",
                        "holding_started_at": utc_time(event.holding_started_at).isoformat(),
                    }
                    event.data = {**event.data, "exit_task": task, "risk_exit": reason}
                    self.store.audit(session, account.id, "ECR_EXIT_TASK_CONFIRMED", task, now)
                buys = [o for o in own_active if o.side == "BUY"]
                if buys:
                    cancellations.extend(o.id for o in buys)
                    continue
                waiting = session.scalar(select(ExecutionCommand.id).where(
                    ExecutionCommand.account_id == account.id,
                    ExecutionCommand.strategy_id == IDENTITY[0],
                    ExecutionCommand.instrument_id == iid,
                    ExecutionCommand.state == "WAITING_ALLOCATION",
                    ExecutionCommand.expires_at > now,
                    ExecutionCommand.payload["action"].as_string() == "REDUCE_TO_TARGET",
                ).limit(1))
                if waiting:
                    continue
                child_cash = self.parameters.initial_capital * core.DEFAULT_POLICY.child_fraction
                if self.config.risk.max_order_notional is not None:
                    child_cash = min(child_cash, self.config.risk.max_order_notional)
                if holding_parameters.economic_revision == "joint_v2":
                    child_cash = min(child_cash, holding_parameters.child_cash)
                quantity = core.floor_lot(min(first.quantity, child_cash / bid), inst.step)
                if quantity < inst.step:
                    continue
                position_qty = sum((lot.quantity for lot, _ in lots), ZERO)
                note = {
                    **frozen, "kind": "RISK_EXIT", "reason": reason,
                    "signal_only": True, "exit_task_id": task["id"],
                }
                exits.append(
                    Intent(
                        schema_version="1.0",
                        intent_id="SIG-ECR11-EXIT-"
                        + content_hash([iid, now, reason, position_qty]),
                        synthetic=account.id.startswith("SIM-"),
                        account_id=account.id,
                        tournament_id=account.tournament,
                        instrument_id=iid,
                        outcome_side="YES",
                        action="REDUCE_TO_TARGET",
                        target_quantity=position_qty - quantity,
                        limit_price=bid,
                        max_order_notional=quantity * bid,
                        created_at=now,
                        expires_at=now + timedelta(minutes=5),
                        rules_hash=inst.rules_hash,
                        mapping_hash=inst.mapping_hash,
                        strategy_id=IDENTITY[0],
                        strategy_version=IDENTITY[1],
                        note=json.dumps(note, default=str, sort_keys=True),
                    )
                )
        for oid in set(cancellations):
            self.queue.cancel(oid, now)
        for intent in exits:
            self.queue.publish(intent)
        return {"cancel_proposals": len(set(cancellations)), "exit_proposals": len(exits)}

    def close(self):
        self.store.engine.dispose()
