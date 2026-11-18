"""Additional ECR checks inside the existing Planner/Executor transactions.

There is no exchange client here. Synthetic intents use the shared OMS and ledger.
Real and synthetic intents use the same reservation and execution hooks.
"""

from __future__ import annotations

import json
from dataclasses import asdict, fields, replace
from datetime import timedelta
from decimal import Decimal

from sqlalchemy import select

from sigbot.domain import ZERO, Intent, content_hash, utc_time
from sigbot.errors import RiskRejected, SchemaMismatch
from sigbot.risk import Planner
from sigbot.storage import (
    Account,
    EconomicEvent,
    FillRow,
    Instrument,
    IntentRow,
    Order,
    Position,
    PositionLot,
    StrategyEvent,
)
from sigbot.strategies.ecr_sig_v1 import core
from sigbot.strategies.ecr_sig_v1.inputs import AccountInput, CandidateInput, Snapshot, epoch
from sigbot.strategies.registry import ECR


def proposal_from_json(data: dict) -> core.Plan:
    values = dict(data)
    for f in fields(core.Plan):
        if f.type == "Decimal":
            values[f.name] = Decimal(values[f.name])
    return core.Plan(**values)


def entry_intent(snapshot: Snapshot, candidate: CandidateInput, proposal: core.Plan) -> Intent:
    payload = {
        "kind": "ENTRY",
        "candidate": candidate.model_dump(mode="json"),
        "plan": asdict(proposal),
        "account_reference": snapshot.account.model_dump(mode="json"),
    }
    return Intent.model_validate(
        {
            "schema_version": "1.0",
            "intent_id": ("SIM-ECR-" if snapshot.synthetic else "SIG-ECR-")
            + content_hash(
                [
                    ECR.strategy_id,
                    ECR.version,
                    snapshot.account_id,
                    snapshot.tournament_id,
                    proposal.key,
                    proposal.created_at,
                ]
            ),
            "synthetic": snapshot.synthetic,
            "account_id": snapshot.account_id,
            "tournament_id": snapshot.tournament_id,
            "instrument_id": candidate.instrument_id,
            "outcome_side": candidate.side,
            "action": "INCREASE_TO_TARGET",
            "target_quantity": proposal.quantity,
            "limit_price": proposal.limit_price,
            "max_order_notional": proposal.total_reserved,
            "created_at": snapshot.now,
            "expires_at": snapshot.now + timedelta(seconds=5),
            "rules_hash": candidate.rules_current_hash,
            "mapping_hash": candidate.review.mapping_hash,
            "strategy_id": ECR.strategy_id,
            "strategy_version": ECR.version,
            "note": json.dumps(payload, default=str, sort_keys=True),
        }
    )


class EcrPlanner(Planner):
    def envelope(self, intent: Intent) -> dict:
        if (intent.strategy_id, intent.strategy_version) != (
            ECR.strategy_id,
            ECR.version,
        ):
            raise RiskRejected("ECR_STRATEGY_SCOPE_MISMATCH")
        if intent.note is None:
            raise RiskRejected("ECR_PLAN_METADATA_REQUIRED")
        data = json.loads(intent.note)
        if data.get("kind") not in {"ENTRY", "EXIT"}:
            raise RiskRejected("ECR_PLAN_METADATA_REQUIRED")
        return data

    def event(self, s, account_id: str, event_key: str) -> StrategyEvent | None:
        return s.get(StrategyEvent, (account_id, ECR.strategy_id, ECR.version, event_key))

    def shared_account(
        self, s, account: Account, reference: AccountInput, exclude: Order | None = None
    ) -> core.Account:
        result = reference.account()
        result.cash = account.cash
        result.total_exposure = result.reserved_and_future_fees = ZERO
        (
            result.race_exposure,
            result.state_exposure,
            result.shock_exposure,
            result.direction_exposure,
        ) = {}, {}, {}, {}
        result.occupied_events = set()
        all_events = list(
            s.scalars(select(StrategyEvent).where(StrategyEvent.account_id == account.id))
        )
        event_by_inst = {e.instrument_id: e for e in all_events if e.state != "CLOSED"}
        instruments = {i.id: i for i in s.scalars(select(Instrument))}
        positions = list(s.scalars(select(Position).where(Position.account_id == account.id)))
        orders = list(s.scalars(select(Order).where(Order.account_id == account.id)))
        result.unknown_writes = any(
            o.request_state in {"UNKNOWN", "SENDING"} or o.cancel_state == "CANCEL_UNKNOWN"
            for o in orders
        )
        result.reconciled = account.lifecycle == "HEALTHY"
        conservative_equity = account.cash

        def add(instrument_id: str, amount: Decimal):
            if amount <= ZERO:
                return
            inst = instruments[instrument_id]
            event = event_by_inst.get(instrument_id)
            event_key = event.event_key if event else inst.event_group or "UNKNOWN"
            result.total_exposure += amount
            for mapping, key in (
                (result.race_exposure, event_key),
                (result.state_exposure, event.state_group if event else "UNKNOWN"),
                (result.shock_exposure, event.shock_group if event else "UNKNOWN"),
                (result.direction_exposure, event.direction_bucket if event else "UNKNOWN"),
            ):
                mapping[key] = mapping.get(key, ZERO) + amount
            result.occupied_events.add(event_key)

        for pos in positions:
            if pos.quantity <= ZERO:
                continue
            if pos.cost is None:
                raise RiskRejected("ECR_UNKNOWN_ACCOUNT_COST")
            add(pos.instrument_id, pos.cost)
            inst = instruments[pos.instrument_id]
            if inst.fee_per_share is None:
                raise RiskRejected("ECR_UNKNOWN_ACCOUNT_FEE")
            try:
                bid_levels = tuple(
                    core.Level(Decimal(x["price"]), Decimal(x["quantity"]))
                    for x in inst.book.get("bids", [])
                )
                if (
                    inst.book_at is None
                    or not 0 <= (self.clock.now - utc_time(inst.book_at)).total_seconds() <= 5
                ):
                    continue  # No liquidity mark raises the budget.
                core.consume(bid_levels, pos.quantity, False)  # Full liquidation depth required.
                remaining, notional = pos.quantity, ZERO
                for level in bid_levels:
                    take = min(remaining, level.size)
                    notional += take * level.price
                    remaining -= take
                    if not remaining:
                        break
                # Multiplying a rounded recurring VWAP back by its quantity can
                # invent sub-unit noise. Sum actual Decimal notionals instead.
                conservative_equity += max(ZERO, notional - pos.quantity * inst.fee_per_share)
            except core.Reject:
                pass  # Missing full liquidation depth marks this position at zero.
        for order in orders:
            if exclude is not None and order.id == exclude.id:
                continue
            if order.reserve_cash:
                add(order.instrument_id, order.reserve_cash)
                result.reserved_and_future_fees += order.reserve_cash
        for event in all_events:
            if (
                exclude is not None
                and event.instrument_id == exclude.instrument_id
                and exclude.side == "BUY"
            ):
                continue
            if event.state != "CLOSED":
                result.occupied_events.add(event.event_key)
                add(event.instrument_id, event.future_fee_reserve)
                result.reserved_and_future_fees += event.future_fee_reserve
        result.equity = min(result.equity, conservative_equity)
        return result

    def proposal(
        self, s, account: Account, intent: Intent, exclude: Order | None = None
    ) -> core.Plan:
        data = self.envelope(intent)
        raw = CandidateInput.model_validate_json(json.dumps(data["candidate"]))
        if raw.instrument_id != intent.instrument_id or raw.side != intent.outcome_side:
            raise RiskRejected("ECR_CANDIDATE_IDENTITY_MISMATCH")
        candidate = raw.candidate(self.clock.now, intent.synthetic)
        inst = s.get(Instrument, intent.instrument_id)
        if (
            inst is None
            or inst.event_group != candidate.event_key
            or inst.exchange_id != raw.exchange_id
        ):
            raise RiskRejected("ECR_EVENT_MAPPING_MISMATCH")
        if (
            inst.fee_per_share != candidate.buy_fee_bound
            or candidate.sell_fee_bound > inst.fee_per_share
        ):
            raise RiskRejected("ECR_FEE_BOUND_MISMATCH")
        if inst.book_at is None:
            raise RiskRejected("ECR_BOOK_NOT_READY")
        book = core.Book(
            tuple(
                core.Level(Decimal(x["price"]), Decimal(x["quantity"]))
                for x in inst.book.get("bids", [])
            ),
            tuple(
                core.Level(Decimal(x["price"]), Decimal(x["quantity"]))
                for x in inst.book.get("asks", [])
            ),
            epoch(utc_time(inst.book_at)),
            inst.quality == "HEALTHY",
        )
        event = self.event(s, account.id, candidate.event_key)
        if event and event.last_exit_at:
            if (
                self.clock.now - utc_time(event.last_exit_at)
            ).total_seconds() < core.DEFAULT_POLICY.max_holding_seconds:
                raise core.Reject("event cooldown")
            candidate = replace(candidate, last_event_exit_at=epoch(utc_time(event.last_exit_at)))
        candidate = replace(candidate, sig_book=book)
        position = s.get(Position, (account.id, intent.instrument_id))
        if position and position.quantity:
            raise RiskRejected("ECR_EXISTING_INSTRUMENT_EXPOSURE")
        reference = AccountInput.model_validate_json(json.dumps(data["account_reference"]))
        computed = core.plan(
            candidate, self.shared_account(s, account, reference, exclude), epoch(self.clock.now)
        )
        declared = proposal_from_json(data["plan"])
        if any(
            getattr(computed, f.name) != getattr(declared, f.name)
            for f in fields(core.Plan)
            if f.name not in {"created_at", "valid_until"}
        ):
            raise RiskRejected("ECR_PLAN_METADATA_CHANGED_REPLAN_REQUIRED")
        if (
            declared.created_at != epoch(intent.created_at)
            or declared.valid_until != declared.created_at + 5
        ):
            raise RiskRejected("ECR_PLAN_TIME_MISMATCH")
        return computed

    def calculate(self, s, account, intent):
        data = self.envelope(intent)
        common = super().calculate(s, account, intent)
        if data["kind"] == "ENTRY":
            try:
                proposal = self.proposal(s, account, intent)
            except core.Reject as exc:
                raise RiskRejected("ECR_REJECTED:" + str(exc)) from None
            if (common.quantity, common.price) != (
                proposal.quantity,
                proposal.limit_price,
            ) or intent.target_quantity != proposal.quantity:
                raise RiskRejected("ECR_FIXED_PLAN_CHANGED_REPLAN_REQUIRED")
        else:
            self.check_exit_ownership(s, account, intent, data)
        return common

    def on_reserved(self, s, account, intent, plan, order_id):
        data = self.envelope(intent)
        if data["kind"] == "EXIT":
            event = self.event(s, account.id, data["event_key"])
            assert event is not None
            event.state = "EXIT_PENDING"
            return
        raw = CandidateInput.model_validate_json(json.dumps(data["candidate"]))
        event_key = raw.event.event_key(intent.synthetic)
        event = self.event(s, account.id, event_key)
        if event is None:
            event = StrategyEvent(
                account_id=account.id,
                strategy_id=ECR.strategy_id,
                strategy_version=ECR.version,
                event_key=event_key,
            )
            s.add(event)
        event.instrument_id = intent.instrument_id
        event.state, event.state_group = "RESERVED", raw.event.state
        event.shock_group, event.direction_bucket = (
            raw.shock_group or "UNKNOWN",
            raw.direction_bucket or "UNKNOWN",
        )
        assert raw.review.sell_fee_bound is not None
        event.future_fee_reserve = plan.quantity * raw.review.sell_fee_bound
        event.holding_started_at = None
        event.data = {
            **data,
            "cash_floor": str(
                Decimal(data["account_reference"]["initial"])
                * core.DEFAULT_POLICY.cash_floor_fraction
            ),
        }
        self.store.audit(
            s,
            account.id,
            "STRATEGY_RESERVED",
            {
                "strategy_id": ECR.strategy_id,
                "version": ECR.version,
                "event_key": event_key,
                "order_id": order_id,
            },
            self.clock.now,
        )

    def check_dispatch(self, s, account, intent, order):
        data = self.envelope(intent)
        if data["kind"] == "ENTRY":
            try:
                proposal = self.proposal(s, account, intent, order)
            except core.Reject as exc:
                raise RiskRejected("ECR_SEND_RECHECK:" + str(exc)) from None
            if (proposal.quantity, proposal.limit_price) != (order.quantity, order.price):
                raise RiskRejected("ECR_SEND_REPLAN_REQUIRED")
        else:
            self.check_exit_ownership(s, account, intent, data)

    def check_exit_ownership(self, s, account, intent, data):
        if intent.action != "REDUCE_TO_TARGET":
            raise RiskRejected("ECR_EXIT_MUST_REDUCE")
        event = self.event(s, account.id, data["event_key"])
        if (
            event is None
            or event.instrument_id != intent.instrument_id
            or event.holding_started_at is None
        ):
            raise RiskRejected("ECR_NO_OWN_HOLDING")
        for lot in s.scalars(
            select(PositionLot).where(
                PositionLot.account_id == account.id,
                PositionLot.instrument_id == intent.instrument_id,
            )
        ):
            if not lot.quantity:
                continue
            fill = s.get(FillRow, (account.id, lot.economic_id.removeprefix("fill:")))
            order = s.get(Order, fill.order_id) if fill else None
            owner = s.get(IntentRow, order.intent_id) if order else None
            if owner is None or (
                owner.payload.get("strategy_id"),
                owner.payload.get("strategy_version"),
            ) != (ECR.strategy_id, ECR.version):
                raise RiskRejected("ECR_SHARED_INSTRUMENT_EXIT_FORBIDDEN")

    def on_observed(self, s, account, order, observation):
        row = s.get(IntentRow, order.intent_id)
        assert row is not None
        intent = Intent.model_validate(row.payload)
        if (intent.strategy_id, intent.strategy_version) != (ECR.strategy_id, ECR.version):
            return  # Other strategies' fills are still ingested by the shared ledger.
        data = self.envelope(intent)
        key = data["event_key"] if data["kind"] == "EXIT" else data["plan"]["event_key"]
        event = self.event(s, account.id, key)
        assert event is not None
        pos = s.get(Position, (account.id, order.instrument_id))
        quantity = pos.quantity if pos else ZERO
        if order.side == "BUY" and order.filled:
            times = [utc_time(f.filled_at) for f in observation.fills if f.filled_at is not None]
            if not times or any(t > self.clock.now for t in times):
                raise SchemaMismatch("ECR_FILL_TIME_UNKNOWN_OR_FUTURE")
            first = min(times)
            event.holding_started_at = (
                min(utc_time(event.holding_started_at), first)
                if event.holding_started_at
                else first
            )
        terminal = order.request_state == "RESOLVED"
        raw = CandidateInput.model_validate_json(json.dumps(event.data["candidate"]))
        assert raw.review.sell_fee_bound is not None
        pending = order.quantity - order.filled if order.side == "BUY" and not terminal else ZERO
        event.future_fee_reserve = (quantity + pending) * raw.review.sell_fee_bound
        if quantity:
            event.state = "EXIT_PENDING" if order.side == "SELL" and not terminal else "HOLDING"
            assert pos is not None and pos.cost is not None
            proposal = proposal_from_json(event.data["plan"])
            per_share = pos.cost / quantity
            gain = (proposal.external_reference - per_share - raw.review.sell_fee_bound) / 2
            friction = per_share - proposal.current_exit_net
            stop = max(Decimal("0.02"), 2 * friction, gain / 2)
            event.data = {
                **event.data,
                "holding_plan": asdict(
                    replace(
                        proposal,
                        quantity=quantity,
                        entry_cost_bound_per_share=per_share,
                        target_gain_per_share=gain,
                        stop_trigger_per_share=stop,
                    )
                ),
            }
            event.data = json.loads(json.dumps(event.data, default=str))
        elif terminal:
            event.state = "CLOSED"
            if event.holding_started_at is not None:
                event.last_exit_at = self.clock.now
        self.store.audit(
            s,
            account.id,
            "STRATEGY_OBSERVED",
            {
                "strategy_id": ECR.strategy_id,
                "version": ECR.version,
                "event_key": key,
                "state": event.state,
                "holding_started_at": event.holding_started_at.isoformat()
                if event.holding_started_at
                else None,
            },
            self.clock.now,
        )

    def on_unsent_expired(self, s, account, intent, order):
        data = self.envelope(intent)
        if data["kind"] == "ENTRY":
            event = self.event(s, account.id, data["plan"]["event_key"])
            assert event is not None
            event.state = "CLOSED"
            event.future_fee_reserve = ZERO
        else:
            event = self.event(s, account.id, data["event_key"])
            assert event is not None
            event.state = "HOLDING"

    def sync_settlement(self, event_key: str, event_id: str, settled_at) -> None:
        at = utc_time(settled_at)
        if at > self.clock.now:
            raise RiskRejected("ECR_FUTURE_SETTLEMENT")
        with self.store.transaction() as s:
            account = self.store.lock_account(s, self.config.expected_account_id or "SIM-ACCOUNT")
            event = self.event(s, account.id, event_key)
            receipt = s.get(EconomicEvent, (account.id, event_id))
            if (
                event is None
                or receipt is None
                or receipt.kind != "SETTLEMENT"
                or receipt.payload.get("instrument") != event.instrument_id
            ):
                raise RiskRejected("ECR_SETTLEMENT_RECEIPT_REQUIRED")
            pos = s.get(Position, (account.id, event.instrument_id))
            if pos is None or pos.quantity != ZERO or pos.cost != ZERO:
                raise RiskRejected("ECR_SETTLEMENT_NOT_RECONCILED")
            event.state = "CLOSED"
            event.future_fee_reserve = ZERO
            event.last_exit_at = at
            event.data = {**event.data, "settlement_id": event_id}
            self.store.audit(
                s,
                account.id,
                "STRATEGY_SETTLEMENT_LINKED",
                {
                    "strategy_id": ECR.strategy_id,
                    "version": ECR.version,
                    "event_key": event_key,
                    "event_id": event_id,
                },
                at,
            )

    def exit_intent(
        self,
        event_key: str,
        current_reference: Decimal | None,
        reference_missing_seconds: int = 0,
        force_exit: bool = False,
    ) -> tuple[str | None, Intent | None]:
        with self.store.transaction() as s:
            account = self.store.lock_account(s, self.config.expected_account_id or "SIM-ACCOUNT")
            event = self.event(s, account.id, event_key)
            if event is None or event.state == "CLOSED" or event.holding_started_at is None:
                return "NO_FILLED_HOLDING", None
            inst = s.get(Instrument, event.instrument_id)
            pos = s.get(Position, (account.id, event.instrument_id))
            assert inst is not None and pos is not None
            proposal = proposal_from_json(event.data.get("holding_plan", event.data["plan"]))
            raw = CandidateInput.model_validate_json(json.dumps(event.data["candidate"]))
            assert raw.review.sell_fee_bound is not None
            net_exit = None
            marginal = None
            try:
                if (
                    inst.book_at is None
                    or not 0 <= (self.clock.now - utc_time(inst.book_at)).total_seconds() <= 5
                ):
                    raise core.Reject("stale exit book")
                levels = tuple(
                    core.Level(Decimal(x["price"]), Decimal(x["quantity"]))
                    for x in inst.book.get("bids", [])
                )
                vwap, marginal = core.consume(levels, pos.quantity, False)
                net_exit = vwap - raw.review.sell_fee_bound
            except core.Reject:
                pass
            reason = core.exit_reason(
                proposal,
                net_exit,
                current_reference,
                epoch(self.clock.now),
                force_exit=force_exit,
                rules_changed=inst.rules_hash != raw.rules_current_hash,
                reference_missing_seconds=reference_missing_seconds,
                holding_started_at=epoch(utc_time(event.holding_started_at)),
            )
            if (
                reason == "TIME_EXIT"
                and (self.clock.now - utc_time(event.holding_started_at)).total_seconds()
                < core.DEFAULT_POLICY.max_holding_seconds
            ):
                reason = None
            if reason == "EXIT_BLOCKED":
                event.state = "EXIT_BLOCKED"
                return reason, None
            if reason is None or marginal is None:
                return reason, None
            limit = marginal
            reference = AccountInput.model_validate_json(
                json.dumps(event.data["account_reference"])
            )
            budget = (
                core.DEFAULT_POLICY.child_fraction * self.shared_account(s, account, reference).wref
            )
            quantity = min(
                pos.quantity, core.floor_lot(budget / (limit + raw.review.sell_fee_bound), raw.lot)
            )
            quantity = min(
                quantity, core.floor_lot(core.depth(levels, marginal, core.ONE) / 10, raw.lot)
            )
            if quantity < raw.minimum_quantity:
                event.state = "EXIT_BLOCKED"
                return "EXIT_BLOCKED", None
            payload = {"kind": "EXIT", "event_key": event_key, "reason": reason}
            intent = Intent.model_validate(
                {
                    "schema_version": "1.0",
                    "intent_id": (
                        "SIM-ECR-EXIT-" if account.id.startswith("SIM-") else "SIG-ECR-EXIT-"
                    )
                    + content_hash([account.id, event_key, self.clock.now, pos.quantity, pos.cost]),
                    "synthetic": account.id.startswith("SIM-"),
                    "account_id": account.id,
                    "tournament_id": account.tournament,
                    "instrument_id": inst.id,
                    "outcome_side": inst.outcome,
                    "action": "REDUCE_TO_TARGET",
                    "target_quantity": pos.quantity - quantity,
                    "limit_price": limit,
                    "max_order_notional": quantity * (limit + raw.review.sell_fee_bound),
                    "created_at": self.clock.now,
                    "expires_at": self.clock.now + timedelta(seconds=5),
                    "rules_hash": inst.rules_hash,
                    "mapping_hash": inst.mapping_hash,
                    "strategy_id": ECR.strategy_id,
                    "strategy_version": ECR.version,
                    "note": json.dumps(payload),
                }
            )
            self.check_exit_ownership(s, account, intent, payload)
            return reason, intent
