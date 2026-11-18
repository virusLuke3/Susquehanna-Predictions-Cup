"""ECR 1.1: SIG buys a same-election price discount, through the common OMS.

Polymarket is a read-only price reference, not a hedge or a settlement-equivalence
claim. Reuses ECR sizing/exit economics and grid's FIFO ownership/lifecycle hooks.
"""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import timedelta
from decimal import ROUND_CEILING, Decimal
from pathlib import Path
from typing import Any, Literal

from pydantic import Field, model_validator
from sqlalchemy import select

from sigbot.accounting import strategy_finances
from sigbot.domain import ZERO, Intent, Money, StrictModel, content_hash, utc_time
from sigbot.errors import RiskRejected
from sigbot.storage import Audit, Instrument, Position
from sigbot.strategies.ecr_sig_v1 import core
from sigbot.strategies.ecr_sig_v1.inputs import AccountInput, epoch
from sigbot.strategies.ecr_sig_v1.oms import EcrPlanner
from sigbot.strategies.grid_sig import GridPlanner
from sigbot.uncertainty import check_scope

IDENTITY = ("ecr_sig", "1.1")
REFERENCE_FILE = Path("runtime/live/strategies/ecr_sig_v1/shared-references.local.json")


class Parameters(StrictModel):
    initial_capital: Money = Field(default="100000", gt=0, validate_default=True)
    strategy_cash: Money = Field(default="25000", gt=0, le=30000, validate_default=True)
    order_seconds: int = Field(default=120, ge=1, le=120, strict=True)
    economic_revision: Literal["legacy_v11", "joint_v2"] = "legacy_v11"
    model_file: str = ""
    model_hash: str = ""
    model_min_clusters: int = Field(default=20, ge=20)
    holding_seconds: int = Field(default=21600, ge=300, le=21600)
    stop_quantile: Literal[80, 90, 95] = 90
    signal_risk: Money = Field(default="5", gt=0, validate_default=True)
    total_planned_risk: Money = Field(default="250", gt=0, validate_default=True)
    daily_loss: Money = Field(default="125", gt=0, validate_default=True)
    child_cash: Money = Field(default="250", gt=0, validate_default=True)
    market_cash: Money = Field(default="250", gt=0, validate_default=True)
    state_cash: Money = Field(default="1250", gt=0, validate_default=True)
    direction_cash: Money = Field(default="7500", gt=0, validate_default=True)
    exit_buffer: Money = Field(default="0.005", gt=0, validate_default=True)
    depth_fraction: Money = Field(default="0.10", gt=0, le="0.10", validate_default=True)
    minimum_expected_edge: Money = Field(default="0.005", gt=0, validate_default=True)
    holding_cost_per_hour: Money = Field(default="0.0005", ge=0, validate_default=True)

    @model_validator(mode="after")
    def joint_budget(self):
        if self.economic_revision == "joint_v2" and (
            self.strategy_cash > Decimal("25000") or not self.model_file or len(self.model_hash) != 64
        ):
            raise ValueError("joint_v2 requires a bound forecast artifact and capital <= 25000")
        return self


def reference_signal(c, at, p=core.DEFAULT_POLICY):
    venues = core.check_candidate(c, at, p)
    if venues != ["POLYMARKET"] and venues != ["SIM-VENUE-A"]:
        raise core.Reject("one Polymarket reference required")
    reference = c.external_books[0][1].bid
    if (
        reference - c.sig_book.ask - c.buy_fee_bound - c.sell_fee_bound - p.reference_haircut
        < p.single_gap
    ):
        raise core.Reject("current reference discount below 0.06")
    return core.Signal("SIG_POLYMARKET_PRICE_GAP_V11", p.single_gap, reference, ZERO)


def book(raw, timestamp, *, verified=True):
    def levels(side):
        result = []
        for x in raw.get(side, []):
            size = Decimal(str(x.get("quantity", x.get("size")))) - Decimal(
                str(x.get("own_quantity", "0"))
            )
            if size > 0:
                result.append(core.Level(Decimal(x["price"]), size))
        return tuple(result)

    return core.Book(levels("bids"), levels("asks"), epoch(utc_time(timestamp)), verified)


def candidate(inst, reference, at):
    if (reference.get("instrument_id"), reference.get("exchange_id")) != (
        inst.id,
        inst.exchange_id,
    ):
        raise core.Reject("reference instrument identity mismatch")
    if reference.get("office") not in {"HOUSE", "SENATE", "GOVERNOR"}:
        raise core.Reject("local election reference required")
    if (
        reference.get("operator") != "POLYMARKET"
        or reference.get("outcome_side") != "YES"
        or inst.outcome != "YES"
    ):
        raise core.Reject("YES price-reference binding required")
    if not inst.reviewed or inst.fee_per_share != ZERO or not reference.get("identity_verified"):
        raise core.Reject("existing instrument admission and fixed zero fee required")
    external = reference["book"]
    received = utc_time(external["received_at"])
    started = utc_time(external["request_started_at"])
    if not started <= received <= at or (at - started).total_seconds() > 5:
        raise core.Reject("external request stale or future")
    if external.get("source_at") and utc_time(external["source_at"]) > received:
        raise core.Reject("external source clock invalid")
    stamp = inst.book.get("validated_at") or inst.book_at
    if stamp is None:
        raise core.Reject("SIG verified book missing")
    sig = book(inst.book, stamp, verified=inst.quality == "HEALTHY")
    remote = book(external, external["request_started_at"])
    observation = core.Observation(
        epoch(at),
        sig.bid if sig.bids else ZERO,
        sig.ask if sig.asks else ZERO,
        (("POLYMARKET", remote.bid if remote.bids else ZERO, remote.ask if remote.asks else ZERO),),
    )
    # The reference route does not invent historical windows or a news catalyst.
    # This actual current observation only fills the shared sizing type's fields;
    # reference_signal never consumes baseline/history.
    return core.Candidate(
        inst.id,
        inst.exchange_id,
        reference["state"],
        reference["state"],
        reference["party"],
        reference["office"],
        "YES",
        sig,
        (("POLYMARKET", remote),),
        observation,
        (),
        rules_reviewed=True,
        mapping_reviewed=True,
        rules_unchanged=True,
        fee_verified=True,
        lot=inst.step,
        min_qty=inst.step,
        price_tick=inst.tick,
    )


def proposal(c, account, at, parameters=None):
    if parameters and parameters.economic_revision == "joint_v2":
        from sigbot.strategies.ecr_sig_v1 import joint

        return joint.proposal(c, account, epoch(at), parameters, joint.load_model(parameters, epoch(at)))
    return core.plan(c, account, epoch(at), signal_check=reference_signal)


class ReferencePlanner(GridPlanner):
    identity = IDENTITY
    parameter_class: Any = Parameters
    risk_reasons = {
        "STOP_TRIGGER",
        "REFERENCE_REVERSED",
        "REFERENCE_LOST",
        "TIME_EXIT",
        "CONTROLLED_EXIT",
        "REMAINING_EDGE_EXHAUSTED",
        "MODEL_SIGNAL_UNSUPPORTED",
        "TAKE_PROFIT",
    }

    def rebuild_intent(self, session, account, intent):
        n = self.envelope(intent)
        if n.get("kind") != "ENTRY" and n.get("signal_only"):
            return self.rebuild_exit(session, account, intent, n)
        if n.get("kind") != "ENTRY" or not n.get("signal_only"):
            return intent
        inst = session.get(Instrument, intent.instrument_id)
        reference = self.reference_for(inst)
        p = Parameters.model_validate(n["parameters"])
        try:
            computed = proposal(
                candidate(inst, reference, self.clock.now),
                self.shared_account(session, account, p, inst),
                self.clock.now,
                p,
            )
        except core.Reject as exc:
            raise RiskRejected("ECR11:" + str(exc)) from None
        # Only an unallocated signal is rebuilt. An existing order/UNKNOWN never
        # takes this path. Preserve its logical ID, then freeze a five-second plan.
        rebuilt = make_intent(
            account.id, account.tournament, inst, computed, p, self.clock.now, reference
        )
        return rebuilt.model_copy(update={"intent_id": intent.intent_id})

    def rebuild_exit(self, session, account, intent, note):
        inst = session.get(Instrument, intent.instrument_id)
        lots = self.owned_lots(session, account.id, inst.id)
        event = self.event(session, account.id, inst.exchange_id)
        if not lots or not event or event.holding_started_at is None:
            raise RiskRejected("ECR11_NO_OWNED_EXIT_LOT")
        first, _frozen = lots[0]

        try:
            current = book(inst.book, inst.book.get("validated_at") or inst.book_at)
            current.check(epoch(self.clock.now))
            task = event.data.get("exit_task")
            if not task or task["state"] != "ACTIVE" or task["id"] != note.get("exit_task_id"):
                raise core.Reject("confirmed durable exit task required")
            p = Parameters.model_validate(note["parameters"])
            budget = p.initial_capital * core.DEFAULT_POLICY.child_fraction
            if p.economic_revision == "joint_v2":
                budget = min(budget, p.child_cash)
            if self.config.risk.max_order_notional is not None:
                budget = min(budget, self.config.risk.max_order_notional)
            qty = core.floor_lot(min(
                first.quantity, core.depth(current.bids, current.bid, core.ONE) / 10,
                budget / current.bid,
            ), inst.step)
            if qty < inst.step:
                raise core.Reject("exit below lot after depth and child budget")
        except core.Reject as exc:
            raise RiskRejected("ECR11:" + str(exc)) from None
        position = session.get(Position, (account.id, inst.id))
        return intent.model_copy(update={
            "target_quantity": position.quantity - qty, "limit_price": current.bid,
            "max_order_notional": qty * current.bid, "created_at": self.clock.now,
            "expires_at": self.clock.now + timedelta(seconds=5),
            "note": json.dumps({**note, "signal_only": False}, default=str, sort_keys=True),
        })

    def validate_signal(self, session, account, intent, price, quantity, *, dispatch=False):
        if self.envelope(intent).get("signal_only"):
            raise RiskRejected("ECR11_SIGNAL_REQUIRES_ALLOCATION_REPLAN")
        return super().validate_signal(session, account, intent, price, quantity, dispatch=dispatch)

    def reference_for(self, inst):
        frame = json.loads(REFERENCE_FILE.read_text())
        if (frame.get("account_id"), frame.get("tournament_id")) != (
            self.config.expected_account_id,
            self.config.expected_tournament_id,
        ):
            raise RiskRejected("ECR11_REFERENCE_ACCOUNT_MISMATCH")
        published = frame.get("books_by_instrument", {}).get(inst.id, {})
        verified = getattr(self.store, "ecr_reference_books", {}).get(inst.id)
        if verified and (
            not published
            or verified["binding"] == published["binding"]
            and utc_time(verified["book"]["request_started_at"])
            >= utc_time(published["book"]["request_started_at"])
        ):
            return verified
        return published

    def shared_account(self, session, account, p, inst, *, exclude=None):
        check_scope(session, account, inst, self.config.parallel_strategy_execution)
        reference = AccountInput(
            initial=p.initial_capital,
            cash=account.cash,
            equity=p.initial_capital,
            high_water=p.initial_capital,
            day_start_equity=p.initial_capital,
        )
        result = EcrPlanner(
            self.store, self.config, self.clock, self.starts_at, self.ends_at
        ).shared_account(session, account, reference, exclude)
        previous = session.scalar(
            select(Audit)
            .where(Audit.account_id == account.id, Audit.kind == "ECR_RUNTIME_RISK_REFERENCE")
            .order_by(Audit.id.desc())
            .limit(1)
        )
        data = previous.data if previous else {}
        if Decimal(data.get("initial", str(p.initial_capital))) != p.initial_capital:
            raise RiskRejected("ECR_RUNTIME_INITIAL_CAPITAL_CHANGED")
        day = self.clock.now.date().isoformat()
        result.high_water = max(result.equity, Decimal(data.get("high_water", str(p.initial_capital))))
        result.day_start_equity = (
            Decimal(data.get("day_start_equity", str(p.initial_capital)))
            if not data or data["utc_day"] == day else result.equity
        )
        updated = {
            "initial": str(p.initial_capital),
            "high_water": str(result.high_water),
            "day_start_equity": str(result.day_start_equity),
            "utc_day": day,
        }
        if updated != data:
            self.store.audit(session, account.id, "ECR_RUNTIME_RISK_REFERENCE", updated, self.clock.now)
        # check_scope above proves all uncertainty is either resolved or covered
        # and isolated outside this instrument/event. Its reserves remain in result.
        result.unknown_writes = False
        if p.economic_revision == "joint_v2":
            result.total_exposure = ZERO
            result.race_exposure, result.state_exposure = {}, {}
            result.shock_exposure, result.direction_exposure = {}, {}
            exposures = [
                (meta["entry_plan"], lot.cost)
                for lot, meta in self.owned_lots(session, account.id)
            ]
            exposures.extend(
                (json.loads(origin.payload["note"])["entry_plan"], order.reserve_cash)
                for order, origin in self.own_orders(session, account.id, active=True)
                if order.side == "BUY" and (exclude is None or order.id != exclude.id)
            )
            for meta, cost in exposures:
                result.total_exposure += cost
                for group, key in (
                    (result.race_exposure, meta["event_key"]),
                    (result.state_exposure, meta["state"]),
                    (result.shock_exposure, meta["shock_group"]),
                    (result.direction_exposure, meta["direction_bucket"]),
                ):
                    group[key] = group.get(key, ZERO) + cost
        return result

    def validate_entry(self, session, account, intent, price, quantity, *, dispatch=False):
        n = self.envelope(intent)
        if n.get("signal_only"):
            raise RiskRejected("ECR11_SIGNAL_REQUIRES_ALLOCATION_REPLAN")
        p = Parameters.model_validate(n["parameters"])
        inst = session.get(Instrument, intent.instrument_id)
        reference = self.reference_for(inst)
        if reference.get("binding") != n.get("reference_binding"):
            raise RiskRejected("ECR11_REFERENCE_IDENTITY_MISMATCH")
        active = [
            (o, r)
            for o, r in self.own_orders(session, account.id, active=True)
            if o.instrument_id == inst.id
        ]
        if intent.action != "INCREASE_TO_TARGET" or self.owned_lots(session, account.id, inst.id):
            raise RiskRejected("ECR11_ONE_ENTRY_NO_AVERAGING")
        if any(o.intent_id != intent.intent_id for o, _ in active):
            raise RiskRejected("ECR11_ENTRY_ALREADY_PENDING")
        event = self.event(session, account.id, inst.exchange_id)
        if (
            event
            and event.last_exit_at
            and (self.clock.now - utc_time(event.last_exit_at)).total_seconds()
            < core.DEFAULT_POLICY.max_holding_seconds
        ):
            raise RiskRejected("ECR11_EVENT_COOLDOWN")
        exclude = (
            next((o for o, _ in active if o.intent_id == intent.intent_id), None)
            if dispatch
            else None
        )
        try:
            computed = proposal(
                candidate(inst, reference, self.clock.now),
                self.shared_account(session, account, p, inst, exclude=exclude),
                self.clock.now,
                p,
            )
        except core.Reject as exc:
            raise RiskRejected("ECR11:" + str(exc)) from None
        # Recheck the complete quantity and current reference, not just best prices.
        if quantity > computed.quantity or price != computed.limit_price:
            raise RiskRejected("ECR11_PLAN_CHANGED_REPLAN_REQUIRED")
        if p.economic_revision != "joint_v2" and (
            Decimal(n["target"]) < price + computed.target_gain_per_share
            or Decimal(n["stop_distance"]) > computed.stop_trigger_per_share
        ):
            raise RiskRejected("ECR11_FROZEN_EXIT_CHANGED")
        funds = strategy_finances(
            session,
            account.id,
            IDENTITY[0],
            p.strategy_cash,
            exclude_order=exclude.id if exclude else None,
        )
        if quantity * price > funds["available"]:
            raise RiskRejected("ECR11_STRATEGY_CAPITAL_EXCEEDED")
        if p.economic_revision == "joint_v2":
            nav = funds["equity_at_fifo_cost"]
            for lot, _meta in self.owned_lots(session, account.id):
                held_inst = session.get(Instrument, lot.instrument_id)
                try:
                    current = book(held_inst.book, held_inst.book.get("validated_at") or held_inst.book_at,
                                   verified=held_inst.quality == "HEALTHY")
                    current.check(epoch(self.clock.now))
                    nav += core.consume(current.bids, lot.quantity, False)[0] * lot.quantity - lot.cost
                except core.Reject:
                    raise RiskRejected("ECR_JOINT_NAV_MARK_UNAVAILABLE") from None
            daily = session.scalar(select(Audit).where(
                Audit.account_id == account.id, Audit.kind == "ECR_JOINT_DAILY_NAV",
            ).order_by(Audit.id.desc()).limit(1))
            if daily is None or daily.data["utc_day"] != self.clock.now.date().isoformat():
                self.store.audit(session, account.id, "ECR_JOINT_DAILY_NAV", {
                    "utc_day": self.clock.now.date().isoformat(), "start_nav": str(nav),
                }, self.clock.now)
                start_nav = nav
            else:
                start_nav = Decimal(daily.data["start_nav"])
            planned = sum((
                lot.quantity * (Decimal(meta["stop_distance"]) + p.exit_buffer)
                for lot, meta in self.owned_lots(session, account.id)
            ), ZERO)
            planned += sum((
                (order.quantity - order.filled) * (
                    Decimal(json.loads(origin.payload["note"])["stop_distance"]) + p.exit_buffer
                )
                for order, origin in self.own_orders(session, account.id, active=True)
                if order.side == "BUY" and order.intent_id != intent.intent_id
            ), ZERO)
            if nav <= start_nav - p.daily_loss or planned + quantity * (
                Decimal(n["stop_distance"]) + p.exit_buffer
            ) > p.total_planned_risk:
                raise RiskRejected("ECR_JOINT_LOSS_OR_PLANNED_RISK_BUDGET")
            if event and event.data.get("exit_task", {}).get("state") == "ACTIVE":
                raise RiskRejected("ECR_DURABLE_EXIT_ACTIVE")

    def check_liquidity(self, inst, side, price, quantity, intent):
        stamp = inst.book.get("validated_at") or inst.book_at
        current = book(inst.book, stamp, verified=inst.quality == "HEALTHY")
        current.check(epoch(self.clock.now))
        levels = current.asks if side == "BUY" else current.bids
        available = (
            sum((x.size for x in levels if x.price <= price), ZERO)
            if side == "BUY"
            else sum((x.size for x in levels if x.price >= price), ZERO)
        )
        if quantity > available / 10:
            raise RiskRejected("ECR11_PROTECTED_DEPTH_EXCEEDED")
        if side == "BUY" and price > current.ask + core.DEFAULT_POLICY.ask_band:
            raise RiskRejected("ECR11_ENTRY_SLIPPAGE")
        if side == "SELL" and price > current.bid:
            raise RiskRejected("ECR11_EXIT_CURRENT_BID_REQUIRED")

    def on_reserved(self, session, account, intent, plan, order_id):
        super().on_reserved(session, account, intent, plan, order_id)
        n = self.envelope(intent)
        inst = session.get(Instrument, intent.instrument_id)
        event = self.event(session, account.id, inst.exchange_id)
        reference = self.reference_for(inst)
        event.state_group = reference.get("state", "UNKNOWN")
        event.shock_group = reference.get("state", "UNKNOWN")
        event.direction_bucket = reference.get("party", "UNKNOWN")
        event.data = {
            **event.data,
            "cash_floor": str(
                Parameters.model_validate(n["parameters"]).initial_capital
                * core.DEFAULT_POLICY.cash_floor_fraction
            ),
            "reference_event": inst.event_group,
            "entry_plan": n.get("entry_plan", event.data.get("entry_plan")),
            "parameters": n["parameters"],
        }

    def update_event(self, session, account, order):
        super().update_event(session, account, order)
        inst = session.get(Instrument, order.instrument_id)
        event = self.event(session, account.id, inst.exchange_id)
        if event and event.state == "CLOSED":
            task = event.data.get("exit_task")
            event.data = {**event.data, "risk_exit": None, **(
                {"exit_task": {**task, "state": "COMPLETE", "completed_at": self.clock.now.isoformat()}}
                if task else {}
            )}


def make_intent(account, tournament, inst, plan, parameters, now, reference):
    forecast = None
    if parameters.economic_revision == "joint_v2":
        from sigbot.strategies.ecr_sig_v1 import joint

        forecast = joint.forecast(candidate(inst, reference, now), plan.quantity,
                                 joint.load_model(parameters, epoch(now)), parameters)
        parameters = parameters.model_copy(update={"holding_seconds": forecast["horizon"]})
    note = {
        "kind": "ENTRY",
        "parameters": parameters.model_dump(mode="json"),
        "exchange_id": inst.exchange_id,
        "reference_binding": reference["binding"],
        "slot": 1,
        "mu": str(plan.external_reference),
        "width": str(plan.target_gain_per_share),
        "target": str(plan.entry_cost_bound_per_share + plan.target_gain_per_share),
        "stop_distance": str(plan.stop_trigger_per_share),
        "entry_plan": asdict(plan),
        "economic_revision": parameters.economic_revision,
        "forecast_at_entry": forecast,
        "settlement_equivalent": "NOT_REQUIRED_FOR_PRICE_REFERENCE",
    }
    return Intent(
        schema_version="1.0",
        intent_id=("SIM-" if account.startswith("SIM-") else "SIG-")
        + "ECR11-"
        + content_hash([account, inst.id, now, reference["binding"]]),
        synthetic=account.startswith("SIM-"),
        account_id=account,
        tournament_id=tournament,
        instrument_id=inst.id,
        outcome_side=inst.outcome,
        action="INCREASE_TO_TARGET",
        target_quantity=plan.quantity,
        limit_price=plan.limit_price,
        max_order_notional=plan.total_reserved,
        created_at=now,
        expires_at=now + timedelta(seconds=5),
        rules_hash=inst.rules_hash,
        mapping_hash=inst.mapping_hash,
        strategy_id=IDENTITY[0],
        strategy_version=IDENTITY[1],
        note=json.dumps(note, default=str, sort_keys=True),
    )


def signal_intent(account, tournament, inst, reference, observed, parameters, now):
    """Durable price signal; the consumer MUST rebuild before any reservation."""
    if (
        observed.get("status") != "FRESH"
        or observed.get("ask") is None
        or observed.get("bid") is None
    ):
        raise core.Reject("SIG observed price missing")
    if not 0 <= (now - utc_time(observed["received_at"])).total_seconds() <= 30:
        raise core.Reject("SIG observed price stale")
    ask, bid = Decimal(observed["ask"]), Decimal(observed["bid"])
    policy = core.DEFAULT_POLICY
    remote = book(reference["book"], reference["book"]["request_started_at"])
    remote.check(epoch(now), max_age=30)
    if (
        not policy.price_low <= ask <= policy.price_high
        or not ZERO < bid < ask
        or ask - bid > policy.sig_spread
        or remote.ask - remote.bid > policy.external_spread
        or remote.bid - ask - policy.reference_haircut < policy.single_gap
    ):
        raise core.Reject("current observed discount below entry guards")
    limit = (ask / inst.tick).to_integral_value(rounding=ROUND_CEILING) * inst.tick
    cash = min(parameters.strategy_cash, parameters.initial_capital * policy.child_fraction)
    if parameters.economic_revision == "joint_v2":
        cash = min(cash, parameters.child_cash, parameters.market_cash)
    note = {
        "kind": "ENTRY",
        "signal_only": True,
        "parameters": parameters.model_dump(mode="json"),
        "exchange_id": inst.exchange_id,
        "reference_binding": reference["binding"],
        "observed_sig_price": observed,
        "allocation": "REQUIRES_CURRENT_VERIFIED_QUANTITY_BOOKS",
    }
    return Intent(
        schema_version="1.0",
        intent_id=("SIM-" if account.startswith("SIM-") else "SIG-")
        + "ECR11-SIGNAL-"
        + content_hash([account, inst.id, now]),
        synthetic=account.startswith("SIM-"),
        account_id=account,
        tournament_id=tournament,
        instrument_id=inst.id,
        outcome_side="YES",
        action="INCREASE_TO_TARGET",
        target_quantity=core.floor_lot(cash / limit, inst.step),
        limit_price=limit,
        max_order_notional=cash,
        created_at=now,
        expires_at=now + timedelta(seconds=30),
        rules_hash=inst.rules_hash,
        mapping_hash=inst.mapping_hash,
        strategy_id=IDENTITY[0],
        strategy_version=IDENTITY[1],
        note=json.dumps(note, default=str, sort_keys=True),
    )
