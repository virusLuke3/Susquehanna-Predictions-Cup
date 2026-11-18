"""PRC policy inside the shared account transaction and sole OMS executor."""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import asdict, fields, replace
from datetime import timedelta
from decimal import Decimal
from typing import Literal

from sqlalchemy import select

from sigbot.domain import ZERO, Intent, content_hash, utc_time
from sigbot.errors import Conflict, RiskRejected, SchemaMismatch
from sigbot.risk import Planner
from sigbot.storage import (
    Audit,
    EconomicEvent,
    FillRow,
    Instrument,
    IntentRow,
    Order,
    PollObservationRow,
    PollReportRow,
    Position,
    PositionLot,
    StrategyDayRisk,
    StrategyEvaluation,
    StrategyEvent,
)
from sigbot.strategies.prc_sig_v1 import core
from sigbot.strategies.prc_sig_v1.models import (
    POLICY,
    AccountInput,
    Book,
    Exposure,
    Level,
    Snapshot,
)
from sigbot.strategies.prc_sig_v1.polls import PollRepository
from sigbot.strategies.registry import PRC


def read_plan(data: dict) -> core.Plan:
    values = dict(data)
    for f in fields(core.Plan):
        if f.type == "Decimal":
            values[f.name] = Decimal(values[f.name])
        elif f.type == "datetime":
            values[f.name] = utc_time(values[f.name])
    return core.Plan(**values)


def entry_intent(snapshot: Snapshot, proposal: core.Plan) -> Intent:
    if not snapshot.synthetic:
        raise RiskRejected("PRC_REAL_OMS_BRIDGE_DISABLED")
    raw = next(c for c in snapshot.candidates if c.key == proposal.key)
    packet = snapshot.model_copy(
        update={
            "candidates": [raw],
            "reports": [
                r
                for r in snapshot.reports
                if any(
                    q.event.event_key(snapshot.synthetic) == proposal.event_key for q in r.questions
                )
            ],
            "comparisons": [p for p in snapshot.comparisons if p.event_key == proposal.event_key],
        }
    ).model_dump(mode="json")
    return Intent.model_validate(
        {
            "schema_version": "1.0",
            "intent_id": "SIM-PRC-"
            + content_hash(
                [
                    snapshot.account_id,
                    PRC.strategy_id,
                    PRC.version,
                    proposal.release_id,
                    proposal.event_key,
                ]
            ),
            "synthetic": True,
            "account_id": snapshot.account_id,
            "tournament_id": snapshot.tournament_id,
            "instrument_id": proposal.instrument_id,
            "outcome_side": proposal.side,
            "action": "INCREASE_TO_TARGET",
            "target_quantity": proposal.quantity,
            "limit_price": proposal.limit_price,
            "max_order_notional": proposal.total_reserved,
            "created_at": proposal.created_at,
            "expires_at": proposal.valid_until,
            "rules_hash": raw.rules_current_hash,
            "mapping_hash": raw.mapping_hash,
            "strategy_id": PRC.strategy_id,
            "strategy_version": PRC.version,
            "note": json.dumps(
                {"kind": "ENTRY", "snapshot": packet, "plan": asdict(proposal)},
                default=str,
                sort_keys=True,
            ),
        }
    )


class PrcPlanner(Planner):
    def event(self, s, account_id, event_key):
        return s.get(StrategyEvent, (account_id, PRC.strategy_id, PRC.version, event_key))

    def evaluation(self, s, account_id, release_id, event_key):
        return s.get(
            StrategyEvaluation, (account_id, PRC.strategy_id, PRC.version, release_id, event_key)
        )

    def envelope(self, intent):
        if (intent.strategy_id, intent.strategy_version) != (
            PRC.strategy_id,
            PRC.version,
        ) or not intent.synthetic:
            raise RiskRejected("PRC_STRATEGY_SCOPE_MISMATCH")
        data = json.loads(intent.note or "{}")
        if data.get("kind") not in {"ENTRY", "EXIT"}:
            raise RiskRejected("PRC_PLAN_METADATA_REQUIRED")
        return data

    def shared_account(self, s, account, reference: AccountInput, exclude=None):
        result = reference.model_copy(deep=True)
        result.cash = account.cash
        result.reserved = ZERO
        result.exposures = []
        result.cooldowns = {}
        result.reconciled = account.lifecycle == "HEALTHY"
        positions = list(s.scalars(select(Position).where(Position.account_id == account.id)))
        orders = list(s.scalars(select(Order).where(Order.account_id == account.id)))
        events = list(
            s.scalars(select(StrategyEvent).where(StrategyEvent.account_id == account.id))
        )
        by_inst = {e.instrument_id: e for e in events if e.state != "CLOSED"}
        insts = {i.id: i for i in s.scalars(select(Instrument))}
        result.unknown_requests = any(
            o.request_state in {"UNKNOWN", "SENDING"} or o.cancel_state == "CANCEL_UNKNOWN"
            for o in orders
        )
        equity = account.cash

        def add(instrument_id, amount, active=True):
            inst = insts[instrument_id]
            event = by_inst.get(instrument_id)
            data = event.data if event else {}
            result.exposures.append(
                Exposure(
                    event_key=event.event_key if event else inst.event_group,
                    state=event.state_group if event else "UNKNOWN",
                    report_id=data.get("release_id", "UNKNOWN"),
                    sample_group=data.get("sample_group", "UNKNOWN"),
                    amount=amount,
                    active=active,
                )
            )

        for pos in positions:
            if not pos.quantity:
                continue
            if pos.cost is None:
                raise RiskRejected("PRC_ACCOUNT_COST_UNKNOWN")
            add(pos.instrument_id, pos.cost)
            inst = insts[pos.instrument_id]
            if inst.fee_per_share is None:
                raise RiskRejected("PRC_ACCOUNT_FEE_UNKNOWN")
            if (
                inst.book_at is not None
                and inst.quality == "HEALTHY"
                and inst.book.get("own_orders_known", False)
                and 0 <= (self.clock.now - utc_time(inst.book_at)).total_seconds() <= 5
                and 0
                <= (
                    self.clock.now - utc_time(inst.book.get("source_at", inst.book_at))
                ).total_seconds()
                <= 5
            ):
                try:
                    avg, _ = core.consume(
                        [Level(**x) for x in self.levels(inst, "bids")], pos.quantity, False
                    )
                    equity += pos.quantity * max(ZERO, avg - inst.fee_per_share)
                except RiskRejected:
                    pass  # Missing complete liquidation depth marks at zero, never up.
        for order in orders:
            if exclude and order.id == exclude.id:
                continue
            if order.reserve_cash:
                add(order.instrument_id, order.reserve_cash)
                result.reserved += order.reserve_cash
        for event in events:
            if event.last_exit_at:
                old = result.cooldowns.get(event.event_key)
                result.cooldowns[event.event_key] = (
                    max(old, utc_time(event.last_exit_at)) if old else utc_time(event.last_exit_at)
                )
            if event.state == "CLOSED" or (
                exclude and event.instrument_id == exclude.instrument_id and exclude.side == "BUY"
            ):
                continue
            add(event.instrument_id, event.future_fee_reserve)
            result.reserved += event.future_fee_reserve
        result.equity = min(result.equity, equity)
        days = list(
            s.scalars(
                select(StrategyDayRisk).where(
                    StrategyDayRisk.account_id == account.id,
                    StrategyDayRisk.utc_day == self.clock.now.date().isoformat(),
                )
            )
        )
        result.daily_committed = sum((day.committed for day in days), ZERO)
        represented = {(day.strategy_id, day.strategy_version) for day in days}
        committed_at = {
            row.data["order"]: row.at
            for row in s.scalars(
                select(Audit).where(Audit.account_id == account.id, Audit.kind == "INTENT_RESERVED")
            )
        }
        # Older registered/manual policies have no day accumulator. Their persisted
        # BUY commitments still count after fills, cancellation or unsent expiry.
        for order in orders:
            owner = s.get(IntentRow, order.intent_id)
            if owner is None:
                raise RiskRejected("PRC_FOREIGN_INTENT_PROVENANCE_UNKNOWN")
            identity = (owner.payload.get("strategy_id"), owner.payload.get("strategy_version"))
            if (
                order.side != "BUY"
                or identity == (PRC.strategy_id, PRC.version)
                or identity in represented
            ):
                continue
            if order.id not in committed_at:
                raise RiskRejected("PRC_FOREIGN_COMMIT_TIME_UNVERIFIED")
            if utc_time(committed_at[order.id]).date() != self.clock.now.date():
                continue
            fee = insts[order.instrument_id].fee_per_share
            if fee is None:
                raise RiskRejected("PRC_FOREIGN_COMMITTED_FEE_UNKNOWN")
            if identity == ("ecr_sig", "1.0"):
                fee = max(
                    fee, Decimal(json.loads(owner.payload["note"])["plan"]["future_fee_per_share"])
                )
            result.daily_committed += order.quantity * (order.price + fee) + order.fee_bound
        if exclude:
            row = s.get(IntentRow, exclude.intent_id)
            if row and row.payload.get("strategy_id") == PRC.strategy_id and exclude.side == "BUY":
                proposal = read_plan(json.loads(row.payload["note"])["plan"])
                if proposal.created_at.date() == self.clock.now.date():
                    result.daily_committed -= proposal.total_reserved
        return result

    def proposal(self, s, account, intent, exclude=None):
        data = self.envelope(intent)
        original = Snapshot.model_validate_json(json.dumps(data["snapshot"]))
        candidate = original.candidates[0]
        declared = read_plan(data["plan"])
        if (candidate.instrument_ids.get(intent.outcome_side), candidate.event.event_key(True)) != (
            intent.instrument_id,
            declared.event_key,
        ):
            raise RiskRejected("PRC_INTENT_IDENTITY_MISMATCH")
        old = self.evaluation(s, account.id, candidate.release_id, declared.event_key)
        if old and (exclude is None or old.order_id != exclude.id):
            raise RiskRejected("PRC_RELEASE_EVENT_ALREADY_EVALUATED")
        report = s.get(PollReportRow, ("SIM", candidate.release_id))
        if report is None or report.data.get("invalidated"):
            raise RiskRejected("PRC_ORIGINAL_REPORT_MISSING_OR_INVALIDATED")
        for later in s.scalars(
            select(PollReportRow)
            .join(
                PollObservationRow,
                (PollReportRow.namespace == PollObservationRow.namespace)
                & (PollReportRow.release_id == PollObservationRow.release_id),
            )
            .where(
                PollObservationRow.namespace == "SIM",
                PollObservationRow.event_key == declared.event_key,
            )
        ):
            if (
                later.release_id != candidate.release_id
                and later.first_public_at
                and report.first_public_at
                and later.first_public_at > report.first_public_at
                and later.available_at <= self.clock.now
            ):
                raise RiskRejected("PRC_INTERVENING_EVENT_REPORT")
        for source in original.reports:
            stored = s.get(PollReportRow, ("SIM", source.release_id))
            if stored is None or stored.digest != source.digest or stored.data.get("invalidated"):
                raise RiskRejected("PRC_POLL_SERIES_MISSING_OR_INVALIDATED")
        inst = s.get(Instrument, intent.instrument_id)
        if inst is None or (inst.exchange_id, inst.event_group) != (
            candidate.exchange_id,
            declared.event_key,
        ):
            raise RiskRejected("PRC_SIG_EVENT_IDENTITY_MISMATCH")
        if (
            inst.fee_per_share != candidate.review.buy_fee_bound
            or candidate.review.sell_fee_bound != inst.fee_per_share
        ):
            raise RiskRejected("PRC_SIG_FEE_BOUND_MISMATCH")
        if inst.book_at is None:
            raise RiskRejected("PRC_BOOK_MISSING")
        current = Book(
            bids=[Level(**x) for x in self.levels(inst, "bids")],
            asks=[Level(**x) for x in self.levels(inst, "asks")],
            source_at=utc_time(inst.book.get("source_at", inst.book_at)),
            received_at=utc_time(inst.book_at),
            verified_at=utc_time(inst.book_at),
            version=str(inst.book.get("version", "LOCAL_VERIFIED")),
            gap_free=inst.quality == "HEALTHY",
            own_orders_known=inst.book.get("own_orders_known", False),
        )
        other_side: Literal["YES", "NO"] = "NO" if intent.outcome_side == "YES" else "YES"
        other_inst = s.get(Instrument, candidate.instrument_ids[other_side])
        if other_inst is None or other_inst.book_at is None:
            raise RiskRejected("PRC_OTHER_OUTCOME_BOOK_MISSING")
        other = Book(
            bids=[Level(**x) for x in self.levels(other_inst, "bids")],
            asks=[Level(**x) for x in self.levels(other_inst, "asks")],
            source_at=utc_time(other_inst.book.get("source_at", other_inst.book_at)),
            received_at=utc_time(other_inst.book_at),
            verified_at=utc_time(other_inst.book_at),
            version=str(other_inst.book.get("version", "LOCAL_VERIFIED")),
            gap_free=other_inst.quality == "HEALTHY",
            own_orders_known=other_inst.book.get("own_orders_known", False),
        )
        candidate = candidate.model_copy(
            update={"books": {intent.outcome_side: current, other_side: other}}
        )
        current_snapshot = original.model_copy(
            update={
                "now": self.clock.now,
                "candidates": [candidate],
                "account": self.shared_account(s, account, original.account, exclude),
            }
        )
        computed = core.plan(candidate, current_snapshot)
        if any(
            getattr(computed, f.name) != getattr(declared, f.name)
            for f in fields(core.Plan)
            if f.name not in {"created_at", "valid_until"}
        ):
            raise RiskRejected("PRC_FIXED_PLAN_CHANGED_REPLAN_FORBIDDEN")
        if declared.created_at != intent.created_at or declared.valid_until != intent.expires_at:
            raise RiskRejected("PRC_PLAN_TIME_MISMATCH")
        return computed

    @staticmethod
    def levels(inst, side):
        return [
            {"price": x["price"], "quantity": str(x["quantity"])}
            for x in inst.book.get(side, [])
            if not x.get("own", False)
        ]

    def calculate(self, s, account, intent):
        common = super().calculate(s, account, intent)
        data = self.envelope(intent)
        if data["kind"] == "ENTRY":
            proposal = self.proposal(s, account, intent)
            if (common.quantity, common.price, intent.target_quantity) != (
                proposal.quantity,
                proposal.limit_price,
                proposal.quantity,
            ):
                raise RiskRejected("PRC_REQUEST_EXCEEDS_FIXED_QUANTITY")
        else:
            self.check_exit(s, account, intent, data)
        return common

    def on_reserved(self, s, account, intent, common, order_id):
        data = self.envelope(intent)
        if data["kind"] == "EXIT":
            event = self.event(s, account.id, data["event_key"])
            event.state = "EXIT_PENDING"
            event.data = {**event.data, "exit_attempts": event.data.get("exit_attempts", 0) + 1}
            return
        proposal = read_plan(data["plan"])
        snapshot = Snapshot.model_validate_json(json.dumps(data["snapshot"]))
        event = self.event(s, account.id, proposal.event_key)
        if event is None:
            event = StrategyEvent(
                account_id=account.id,
                strategy_id=PRC.strategy_id,
                strategy_version=PRC.version,
                event_key=proposal.event_key,
                instrument_id=intent.instrument_id,
                state="RESERVED",
                state_group=proposal.state,
                shock_group=proposal.release_id,
                direction_bucket=proposal.sample_group,
                future_fee_reserve=ZERO,
                data={},
            )
            s.add(event)
        event.instrument_id = intent.instrument_id
        event.state = "RESERVED"
        event.state_group = proposal.state
        event.shock_group = proposal.release_id
        event.direction_bucket = proposal.sample_group
        event.holding_started_at = None
        event.future_fee_reserve = proposal.quantity * proposal.future_fee_per_share
        ref = min(snapshot.account.initial, snapshot.account.equity)
        event.data = {
            **data,
            "release_id": proposal.release_id,
            "sample_group": proposal.sample_group,
            "exit_attempts": 0,
            "cash_floor": str(POLICY.cash_floor * snapshot.account.initial),
            "account_limits": {
                "total_cost": str(POLICY.total_fraction * ref),
                "event_cost": str(POLICY.single_fraction * ref),
                "events": POLICY.max_events,
            },
        }
        s.add(
            StrategyEvaluation(
                account_id=account.id,
                strategy_id=PRC.strategy_id,
                strategy_version=PRC.version,
                release_id=proposal.release_id,
                event_key=proposal.event_key,
                evaluated_at=self.clock.now,
                state="RESERVED",
                reason=None,
                order_id=order_id,
                data={"candidate": proposal.key},
            )
        )
        day_key = (account.id, PRC.strategy_id, PRC.version, self.clock.now.date().isoformat())
        day = s.get(StrategyDayRisk, day_key)
        if day is None:
            day = StrategyDayRisk(
                account_id=account.id,
                strategy_id=PRC.strategy_id,
                strategy_version=PRC.version,
                utc_day=day_key[-1],
                committed=ZERO,
            )
            s.add(day)
        day.committed += proposal.total_reserved

    def allocate_and_reserve(self, snapshot: Snapshot) -> dict:
        if not snapshot.synthetic or snapshot.now != self.clock.now:
            raise RiskRejected("PRC_LOCAL_ALLOCATOR_SCOPE_OR_TIME_MISMATCH")
        rejected, accepted = {}, []
        with self.store.transaction() as s:
            account = self.store.lock_account(s, snapshot.account_id)
            repo = PollRepository(self.store)
            for report in snapshot.reports:
                repo.ingest_locked(s, report)
            candidates = []
            failed = []
            initial = snapshot.model_copy(
                update={"account": self.shared_account(s, account, snapshot.account)}
            )
            for candidate in snapshot.candidates:
                try:
                    signal = core.evaluate(candidate, snapshot)
                    proposal = core.plan(candidate, initial, signal)
                    candidates.append((proposal.score, candidate, signal))
                except RiskRejected as exc:
                    rejected[candidate.key] = exc.code
                    failed.append((candidate, exc.code))
            eligible_keys = {(c.release_id, c.event.event_key(True)) for _, c, _ in candidates}
            for candidate, reason in failed:
                source = next(
                    (r for r in snapshot.reports if r.release_id == candidate.release_id), None
                )
                key = candidate.event.event_key(True)
                if (
                    source
                    and source.original.published_at
                    and snapshot.now >= source.original.published_at + timedelta(minutes=15)
                    and (candidate.release_id, key) not in eligible_keys
                    and not self.evaluation(s, account.id, candidate.release_id, key)
                ):
                    s.add(
                        StrategyEvaluation(
                            account_id=account.id,
                            strategy_id=PRC.strategy_id,
                            strategy_version=PRC.version,
                            release_id=candidate.release_id,
                            event_key=key,
                            evaluated_at=self.clock.now,
                            state="REJECTED",
                            reason=reason,
                            order_id=None,
                            data={"candidate": candidate.key},
                        )
                    )
                    s.flush()
            for _, candidate, signal in sorted(candidates, key=lambda x: (-x[0], x[1].key)):
                if self.evaluation(s, account.id, signal.release_id, signal.event_key):
                    rejected[candidate.key] = "PRC_RELEASE_EVENT_ALREADY_EVALUATED"
                    continue
                try:
                    current = snapshot.model_copy(
                        update={"account": self.shared_account(s, account, snapshot.account)}
                    )
                    proposal = core.plan(candidate, current, signal)
                    oid = self.reserve_locked(s, account, entry_intent(snapshot, proposal))
                    assert oid is not None
                    accepted.append({"order_id": oid, **asdict(proposal)})
                except (RiskRejected, Conflict) as exc:
                    rejected[candidate.key] = exc.code
                    s.add(
                        StrategyEvaluation(
                            account_id=account.id,
                            strategy_id=PRC.strategy_id,
                            strategy_version=PRC.version,
                            release_id=signal.release_id,
                            event_key=signal.event_key,
                            evaluated_at=self.clock.now,
                            state="REJECTED",
                            reason=exc.code,
                            order_id=None,
                            data={"candidate": candidate.key},
                        )
                    )
                s.flush()
            remaining = self.shared_account(s, account, snapshot.account)
        # Includes the pure funnel; persisted allocation is the authoritative plan count.
        result = snapshot.plan()
        result["funnel"]["allocated_events"] = len(accepted)
        result.update(
            plans=accepted,
            rejected=rejected,
            rejection_counts=dict(Counter(rejected.values())),
            total_reserved=sum((p["total_reserved"] for p in accepted), ZERO),
            cash_after_all_reserved=remaining.cash - remaining.reserved,
            daily_committed_after=remaining.daily_committed,
            allocated_events=len(accepted),
            persistent_once_latches=True,
        )
        return result

    def check_dispatch(self, s, account, intent, order):
        data = self.envelope(intent)
        if data["kind"] == "ENTRY":
            proposal = self.proposal(s, account, intent, order)
            if (order.quantity, order.price) != (proposal.quantity, proposal.limit_price):
                raise RiskRejected("PRC_SEND_FIXED_PLAN_MISMATCH")
        else:
            self.check_exit(s, account, intent, data)

    def check_exit(self, s, account, intent, data):
        event = self.event(s, account.id, data["event_key"])
        if (
            intent.action != "REDUCE_TO_TARGET"
            or event is None
            or event.instrument_id != intent.instrument_id
            or event.holding_started_at is None
        ):
            raise RiskRejected("PRC_EXIT_NOT_OWN_REDUCE_ONLY")
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
            ) != (PRC.strategy_id, PRC.version):
                raise RiskRejected("PRC_SHARED_INSTRUMENT_EXIT_FORBIDDEN")

    def on_observed(self, s, account, order, observation):
        row = s.get(IntentRow, order.intent_id)
        intent = Intent.model_validate(row.payload)
        if (intent.strategy_id, intent.strategy_version) != (PRC.strategy_id, PRC.version):
            return
        data = self.envelope(intent)
        key = data["event_key"] if data["kind"] == "EXIT" else data["plan"]["event_key"]
        event = self.event(s, account.id, key)
        pos = s.get(Position, (account.id, order.instrument_id))
        quantity = pos.quantity if pos else ZERO
        if order.side == "BUY" and order.filled:
            times = [utc_time(f.filled_at) for f in observation.fills if f.filled_at is not None]
            if (not times and event.holding_started_at is None) or any(
                t > self.clock.now for t in times
            ):
                raise SchemaMismatch("PRC_FILL_TIME_UNKNOWN_OR_FUTURE")
            if times:
                first = min(times)
                event.holding_started_at = (
                    min(event.holding_started_at, first) if event.holding_started_at else first
                )
        pending = sum(
            (
                o.quantity - o.filled
                for o in s.scalars(
                    select(Order).where(
                        Order.account_id == account.id,
                        Order.instrument_id == event.instrument_id,
                        Order.side == "BUY",
                    )
                )
                if o.reserve_cash
            ),
            ZERO,
        )
        proposal = read_plan(event.data["plan"])
        event.future_fee_reserve = (quantity + pending) * proposal.future_fee_per_share
        if quantity:
            assert pos is not None and pos.cost is not None
            entry = pos.cost / quantity
            # Freeze entry targets at actual first-fill cost; later exits cannot move the goalposts.
            if order.side == "BUY":
                inst = s.get(Instrument, event.instrument_id)
                net = ZERO
                if (
                    inst
                    and inst.book_at
                    and inst.quality == "HEALTHY"
                    and inst.book.get("own_orders_known", False)
                    and 0 <= (self.clock.now - utc_time(inst.book_at)).total_seconds() <= 5
                    and 0
                    <= (
                        self.clock.now - utc_time(inst.book.get("source_at", inst.book_at))
                    ).total_seconds()
                    <= 5
                ):
                    try:
                        avg, _ = core.consume(
                            [Level(**x) for x in self.levels(inst, "bids")], quantity, False
                        )
                        net = avg - proposal.future_fee_per_share
                    except RiskRejected:
                        net = ZERO
                holding = replace(
                    proposal,
                    quantity=quantity,
                    entry_cost_bound=entry,
                    immediate_exit_net=net,
                    stop=max(Decimal("0.02"), Decimal("1.5") * (entry - net)),
                )
                holding = replace(holding, gain=2 * holding.stop)
                event.data = {
                    **event.data,
                    "holding_plan": json.loads(json.dumps(asdict(holding), default=str)),
                    "risk_disposition_required": holding.stop > Decimal("0.06")
                    or holding.entry_cost_bound + holding.gain > 1 - holding.future_fee_per_share,
                }
            event.state = "HOLDING" if order.side == "BUY" else "EXIT_PENDING"
        elif pending:
            event.state = "RESERVED"
        else:
            event.state = "CLOSED"
            if event.holding_started_at:
                event.last_exit_at = self.clock.now
        latch = self.evaluation(s, account.id, proposal.release_id, key)
        if latch:
            latch.state = event.state

    def on_unsent_expired(self, s, account, intent, order):
        data = self.envelope(intent)
        if data["kind"] == "ENTRY":
            event = self.event(s, account.id, data["plan"]["event_key"])
            event.state = "CLOSED"
            event.future_fee_reserve = ZERO
            self.evaluation(
                s, account.id, data["plan"]["release_id"], event.event_key
            ).state = "EXPIRED_UNSENT"
        else:
            self.event(s, account.id, data["event_key"]).state = "HOLDING"

    def exit_intent(self, event_key, force_exit=False):
        if self.config.expected_account_id is None:
            raise RiskRejected("PRC_ACCOUNT_ID_REQUIRED")
        with self.store.transaction() as s:
            account = self.store.lock_account(s, self.config.expected_account_id)
            event = self.event(s, account.id, event_key)
            if event is None or event.holding_started_at is None or event.state == "CLOSED":
                return "NO_FILLED_HOLDING", None
            if event.data.get("exit_attempts", 0) >= 3:
                event.state = "EXIT_BLOCKED"
                return "EXIT_BLOCKED_ATTEMPT_LIMIT", None
            orders = list(
                s.scalars(
                    select(Order).where(
                        Order.account_id == account.id, Order.instrument_id == event.instrument_id
                    )
                )
            )
            if any(
                o.reserve_quantity
                or o.reserve_cash
                or o.request_state in {"UNKNOWN", "SENDING"}
                or o.cancel_state == "CANCEL_UNKNOWN"
                for o in orders
            ):
                return "EXIT_PENDING_OR_UNKNOWN", None
            inst = s.get(Instrument, event.instrument_id)
            pos = s.get(Position, (account.id, event.instrument_id))
            if inst is None or pos is None or pos.quantity <= 0:
                raise RiskRejected("PRC_FILLED_POSITION_NOT_FOUND")
            proposal = read_plan(event.data.get("holding_plan", event.data["plan"]))
            snapshot = Snapshot.model_validate_json(json.dumps(event.data["snapshot"]))
            candidate = snapshot.candidates[0]
            try:
                if (
                    inst.quality != "HEALTHY"
                    or not inst.book.get("own_orders_known", False)
                    or inst.book_at is None
                    or not 0 <= (self.clock.now - utc_time(inst.book_at)).total_seconds() <= 5
                    or not 0
                    <= (
                        self.clock.now - utc_time(inst.book.get("source_at", inst.book_at))
                    ).total_seconds()
                    <= 5
                ):
                    raise RiskRejected("PRC_EXIT_BOOK_STALE_OR_GAPPED")
                levels = [Level(**x) for x in self.levels(inst, "bids")]
                avg, worst = core.consume(levels, pos.quantity, False)
                if worst < max(x.price for x in levels) - Decimal("0.005"):
                    raise RiskRejected("PRC_EXIT_FULL_DEPTH_PROTECTION_FAILED")
            except RiskRejected:
                event.state = "EXIT_BLOCKED"
                return "EXIT_BLOCKED", None
            report = s.get(PollReportRow, ("SIM", proposal.release_id))
            later_poll = any(
                r.release_id != proposal.release_id
                and r.first_public_at
                and r.first_public_at > proposal.created_at
                for r in s.scalars(select(PollReportRow).where(PollReportRow.namespace == "SIM"))
                if any(
                    q.get("event") == candidate.event.model_dump(mode="json")
                    for q in r.data["original"]["questions"]
                )
            )
            net = avg - proposal.future_fee_per_share
            if (
                force_exit
                or report is None
                or report.data.get("invalidated")
                or later_poll
                or inst.rules_hash != candidate.rules_current_hash
                or event.data.get("risk_disposition_required")
            ):
                reason = "CONTROLLED_EXIT"
            elif net - proposal.entry_cost_bound >= proposal.gain:
                reason = "TAKE_PROFIT"
            elif net - proposal.entry_cost_bound <= -proposal.stop:
                reason = "STOP_TRIGGER"
            elif (
                self.clock.now - utc_time(event.holding_started_at)
            ).total_seconds() >= POLICY.holding_seconds:
                reason = "TIME_EXIT"
            else:
                return None, None
            reference = self.shared_account(s, account, snapshot.account)
            budget = POLICY.single_fraction * min(reference.initial, reference.equity)
            qty = core.floor(
                min(
                    pos.quantity,
                    budget / (worst + proposal.future_fee_per_share),
                    Decimal("0.10") * sum((x.quantity for x in levels if x.price >= worst), ZERO),
                ),
                candidate.lot,
            )
            if qty < candidate.minimum_quantity:
                event.state = "EXIT_BLOCKED"
                return "EXIT_BLOCKED", None
            target = Intent.model_validate(
                {
                    "schema_version": "1.0",
                    "intent_id": "SIM-PRC-EXIT-"
                    + content_hash([account.id, event_key, self.clock.now, pos.quantity]),
                    "synthetic": True,
                    "account_id": account.id,
                    "tournament_id": account.tournament,
                    "instrument_id": inst.id,
                    "outcome_side": inst.outcome,
                    "action": "REDUCE_TO_TARGET",
                    "target_quantity": pos.quantity - qty,
                    "limit_price": worst,
                    "max_order_notional": qty * (worst + proposal.future_fee_per_share),
                    "created_at": self.clock.now,
                    "expires_at": self.clock.now + timedelta(seconds=5),
                    "rules_hash": inst.rules_hash,
                    "mapping_hash": inst.mapping_hash,
                    "strategy_id": PRC.strategy_id,
                    "strategy_version": PRC.version,
                    "note": json.dumps({"kind": "EXIT", "event_key": event_key, "reason": reason}),
                }
            )
            self.check_exit(s, account, target, {"event_key": event_key})
            return reason, target

    def sync_settlement(self, event_key, economic_id):
        if self.config.expected_account_id is None:
            raise RiskRejected("PRC_ACCOUNT_ID_REQUIRED")
        with self.store.transaction() as s:
            account = self.store.lock_account(s, self.config.expected_account_id)
            event = self.event(s, account.id, event_key)
            receipt = s.get(EconomicEvent, (account.id, economic_id))
            pos = s.get(Position, (account.id, event.instrument_id)) if event else None
            if (
                event is None
                or receipt is None
                or receipt.kind != "SETTLEMENT"
                or receipt.payload.get("instrument") != event.instrument_id
                or pos is None
                or pos.quantity
                or pos.cost
            ):
                raise RiskRejected("PRC_CONFIRMED_SETTLEMENT_RECEIPT_REQUIRED")
            already_closed = event.state == "CLOSED"
            event.state = "CLOSED"
            event.future_fee_reserve = ZERO
            if not already_closed:
                event.last_exit_at = self.clock.now
            proposal = read_plan(event.data["plan"])
            latch = self.evaluation(s, account.id, proposal.release_id, event_key)
            if latch:
                latch.state = "CLOSED"
            # Cash already came from the common ledger; never post another payout.
