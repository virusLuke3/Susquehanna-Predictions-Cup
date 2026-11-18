"""SSR pair allocation and recovery using the existing account OMS and FIFO ledger.

Two local reservations commit together. Venue executions remain non-atomic.
No HTTP client, credentials, automatic fills, or automatic rule approvals.
"""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from pathlib import Path

from sqlalchemy import select

from sigbot.command_queue import CommandQueue
from sigbot.domain import TERMINAL, ZERO, Intent, content_hash, utc_time
from sigbot.errors import RiskRejected
from sigbot.risk import Planner
from sigbot.storage import Audit, Instrument, IntentRow, Market, Order, Position, StrategyEvent
from sigbot.strategies.semantic_sig_v1.core import number, payouts, sweep
from sigbot.strategies.semantic_sig_v1.replay import consume

IDENTITY = ("ssr_sig", "1.0")


class SsrPlanner(Planner):
    def retryable_unsent(self, session, event):
        """Only locally closed attempts with proof of no HTTP transmission can retry."""
        if event is None or event.state != "CLOSED" or event.data.get("exit_order_ids"):
            return False
        ids = event.data.get("entry_order_ids", [])
        if len(ids) != 2:
            return False
        for key in ids:
            order = session.get(Order, key)
            if (
                not order
                or order.state not in TERMINAL
                or order.request_state != "RESOLVED"
                or order.remote_id
                or order.filled
                or order.reserve_cash
                or order.reserve_quantity
            ):
                return False
            sent = session.scalar(
                select(Audit.id)
                .where(
                    Audit.account_id == event.account_id,
                    Audit.kind == "HTTP_TRANSMISSION_STARTED",
                    Audit.data["order"].as_string() == key,
                )
                .limit(1)
            )
            if sent:
                return False
        return True

    def note(self, intent):
        if (intent.strategy_id, intent.strategy_version) != IDENTITY:
            raise RiskRejected("SSR_STRATEGY_IDENTITY_MISMATCH")
        try:
            note = json.loads(intent.note or "{}")
        except ValueError:
            raise RiskRejected("SSR_INVALID_BASKET_METADATA") from None
        if (
            not isinstance(note, dict)
            or note.get("kind") not in {"ENTRY", "EXIT", "RISK_EXIT"}
            or not note.get("pair_id")
            or not note.get("episode_id")
        ):
            raise RiskRejected("SSR_BASKET_METADATA_REQUIRED")
        return note

    def event(self, session, account_id, pair):
        return session.get(StrategyEvent, (account_id, *IDENTITY, pair))

    def proof(self, session, intent):
        note = self.note(intent)
        inst = session.get(Instrument, intent.instrument_id)
        if inst is None or not isinstance(inst.mapping_evidence, dict):
            raise RiskRejected("SSR_INSTRUMENT_MAPPING_MISSING")
        proof = inst.mapping_evidence.get("ssr_pair_review", {})
        title_review = proof.get("status") == "TITLE_SCOPE_REVIEWED_USER_ACCEPTED"
        if proof.get("status") not in {
            "NORMAL_MUTEX_VERIFIED",
            "TITLE_SCOPE_REVIEWED_USER_ACCEPTED",
        } or content_hash(proof) != note.get("review_hash"):
            raise RiskRejected("SSR_PAIR_RULE_PROOF_UNVERIFIED_OR_CHANGED")
        ids = proof.get("instrument_ids", [])
        instruments = [session.get(Instrument, key) for key in ids]
        if (
            len(ids) != 2
            or len(set(ids)) != 2
            or any(i is None for i in instruments)
            or intent.instrument_id not in ids
            or {proof.get("party_by_instrument", {}).get(key) for key in ids} != {"D", "R"}
            or any(
                i.outcome != "NO"
                or (not title_review and i.event_group != note["pair_id"])
                or i.tournament != intent.tournament_id
                for i in instruments
            )
            or len({i.exchange_id for i in instruments}) != 2
            or len({i.market_id for i in instruments}) != 2
        ):
            raise RiskRejected("SSR_SPECIFIC_RACE_D_R_IDENTITY_REQUIRED")
        if title_review and (
            proof.get("pair_id", instruments[0].event_group) != note["pair_id"]
            or len({i.event_group for i in instruments}) != 1
        ):
            raise RiskRejected("SSR_SPECIFIC_RACE_D_R_IDENTITY_REQUIRED")
        if any(i.mapping_evidence.get("ssr_pair_review") != proof for i in instruments):
            raise RiskRejected("SSR_LEG_REVIEWS_DISAGREE")
        if proof.get("rules_hashes") != {i.id: i.rules_hash for i in instruments} or proof.get(
            "mapping_hashes"
        ) != {i.id: i.mapping_hash for i in instruments}:
            raise RiskRejected("SSR_RULE_OR_MAPPING_CHANGED")
        if not intent.synthetic:
            official_events = []
            for instrument in instruments:
                market = session.get(Market, instrument.market_id)
                nodes = market.raw.get("reviewed_sig_nodes", {}) if market else {}
                details = nodes.get("root", {}).get("contract_details", {})
                event = {
                    k: details.get(k)
                    for k in (
                        "raceId",
                        "stageId",
                        "electionDate",
                        "usState",
                        "raceStage",
                        "resolutionType",
                    )
                }
                if (
                    not market
                    or nodes.get("market_id") != market.api_id
                    or not all(event.values())
                    or event["raceStage"] != "General"
                    or event["resolutionType"] != "Party Winner"
                    or details.get("winnerName")
                    != {"D": "Democratic Party", "R": "Republican Party"}[
                        proof["party_by_instrument"][instrument.id]
                    ]
                ):
                    raise RiskRejected("SSR_OFFICIAL_SPECIFIC_ELECTION_METADATA_REQUIRED")
                official_events.append(event)
            if (
                official_events[0] != official_events[1]
                or proof.get("scope") != "SPECIFIC_ELECTION"
                or proof.get("official_event_identity") != official_events[0]
            ):
                raise RiskRejected("SSR_OFFICIAL_RACE_STAGE_IDENTITY_MISMATCH")
            if (
                not proof.get("reviewed_by")
                or not proof.get("reviewed_at")
                or utc_time(proof["reviewed_at"]) > intent.created_at
                or not proof.get("effective_from")
                or not proof.get("effective_to")
                or not utc_time(proof["effective_from"])
                <= self.clock.now
                < utc_time(proof["effective_to"])
                or not title_review
                and (
                    proof.get("candidate_and_writein_mapping_verified") is not True
                    or proof.get("normal_outcomes_exhaustive") is not True
                )
                or proof.get("extra_fee_per_share") != "0"
                or proof.get("fee_parameter_source") != "USER_FIXED_PARAMETER"
                or proof.get("cash_allocation_approved_for_ssr") is not True
                or proof.get("NO_order_workflow") != "STRICT_POST_OR_VALIDATED_GET_RECOVERY"
            ):
                raise RiskRejected("SSR_LIVE_RULE_COST_CASH_OR_NO_CONTRACT_GATE_MISSING")
            if title_review:
                instruction = Path(proof.get("title_review_instruction_path", ""))
                if not instruction.is_file() or hashlib.sha256(
                    instruction.read_bytes()
                ).hexdigest() != proof.get("title_review_instruction_sha256"):
                    raise RiskRejected("SSR_TITLE_REVIEW_USER_INSTRUCTION_CHANGED")
                accepted = json.loads(instruction.read_text())
                if (
                    accepted.get("source") != "USER_INSTRUCTION"
                    or accepted.get("title_review_sufficient_for_admission") is not True
                    or accepted.get("account_id_hash") != content_hash(intent.account_id)
                    or accepted.get("tournament_id") != intent.tournament_id
                    or proof.get("mutex_basis") != "USER_ACCEPTED_SINGLE_PARTY_WINNER_ASSUMPTION"
                    or proof.get("unconditional_payoff_floor_per_pair") != "0"
                    or proof.get("title_reviewed") is not True
                ):
                    raise RiskRejected("SSR_TITLE_SCOPE_ASSUMPTION_BINDING_REQUIRED")
            receipts = proof.get("source_receipts", [])
            if not receipts:
                raise RiskRejected("SSR_LIVE_SOURCE_RECEIPTS_REQUIRED")
            for receipt in receipts:
                if (
                    not Path(receipt["path"]).is_file()
                    or hashlib.sha256(Path(receipt["path"]).read_bytes()).hexdigest()
                    != receipt["sha256"]
                ):
                    raise RiskRejected("SSR_RULE_SOURCE_RECEIPT_CHANGED")
        affiliations = proof.get("normal_outcome_affiliations")
        if not affiliations or any([party] not in affiliations for party in ["D", "R", "OTHER"]):
            raise RiskRejected("SSR_OTHER_WINNER_MUST_BE_RETAINED")
        if any(
            not isinstance(state, list)
            or not state
            or any(not isinstance(p, str) or p in {"VOID", "N/A", "UNKNOWN"} for p in state)
            for state in affiliations
        ):
            raise RiskRejected("SSR_EXCEPTION_IS_NOT_A_NORMAL_PAYOFF_STATE")
        legs = [
            {"key": key, "party": proof["party_by_instrument"][key], "side": "NO", "quantity": "1"}
            for key in ids
        ]
        floor = min(payouts(legs, [frozenset(state) for state in affiliations]).values())
        if floor != 1:
            raise RiskRejected("SSR_NORMAL_MUTEX_FLOOR_NOT_ONE")
        if not intent.synthetic and any(i.fee_per_share != ZERO for i in instruments):
            raise RiskRejected("SSR_USER_FIXED_ZERO_COST_PARAMETER_CHANGED")
        return proof, instruments

    def current_books(self, instruments):
        self.clock.sync()
        stamps = []
        for inst in instruments:
            if (
                inst.mapping_evidence.get("ssr_pair_review", {}).get("status")
                == "TITLE_SCOPE_REVIEWED_USER_ACCEPTED"
            ):
                reference = inst.mapping_evidence.get("ssr_current_reference", {})
                if (
                    not reference.get("at")
                    or not 0 <= (self.clock.now - utc_time(reference["at"])).total_seconds() <= 180
                    or reference.get("binding") != [inst.rules_hash, inst.mapping_hash]
                ):
                    raise RiskRejected("SSR_CURRENT_RULE_AND_SCOPE_REFERENCE_REQUIRED")
            if (
                inst.quality != "HEALTHY"
                or inst.book_at is None
                or not inst.book.get("own_orders_known")
                or not 0 <= (self.clock.now - utc_time(inst.book_at)).total_seconds() <= 5
                or inst.fee_per_share is None
            ):
                raise RiskRejected("SSR_FRESH_EXTERNAL_BOOK_AND_FEE_REQUIRED")
            stamps.append(utc_time(inst.book_at))
        if (max(stamps) - min(stamps)).total_seconds() > 2:
            raise RiskRejected("SSR_CROSS_LEG_RECEIPT_SKEW")

    def levels(self, inst, side):
        result = []
        for level in inst.book[side]:
            qty = number(level["quantity"])
            if "own_quantity" in level:
                qty -= number(level["own_quantity"])
                if qty < 0:
                    raise RiskRejected("SSR_OWN_QUANTITY_EXCEEDS_AGGREGATE")
            elif level.get("own") is not False:
                raise RiskRejected("SSR_OWN_LEVEL_QUANTITY_UNKNOWN")
            result.append({"price": level["price"], "quantity": str(qty), "own": False})
        return result

    def entry(self, session, account, intent, quantity):
        proof, instruments = self.proof(session, intent)
        self.current_books(instruments)
        exchanges = {i.exchange_id for i in instruments}
        opposite_ids = {
            i.id
            for i in session.scalars(
                select(Instrument).where(
                    Instrument.tournament == account.tournament,
                    Instrument.exchange_id.in_(exchanges),
                    Instrument.outcome == "YES",
                )
            )
        }
        if any(
            p.quantity
            for p in session.scalars(
                select(Position).where(
                    Position.account_id == account.id,
                    Position.instrument_id.in_(opposite_ids),
                )
            )
        ) or any(
            o.state not in TERMINAL
            or o.request_state != "RESOLVED"
            or o.reserve_cash
            or o.reserve_quantity
            for o in session.scalars(
                select(Order).where(
                    Order.account_id == account.id,
                    Order.instrument_id.in_(opposite_ids),
                )
            )
        ):
            raise RiskRejected("SSR_EXISTING_YES_WOULD_BE_OFFSET")
        note = self.note(intent)
        event = self.event(session, account.id, note["pair_id"])
        other = session.scalar(
            select(StrategyEvent).where(
                StrategyEvent.account_id == account.id,
                StrategyEvent.strategy_id == IDENTITY[0],
                StrategyEvent.strategy_version == IDENTITY[1],
                StrategyEvent.event_key != note["pair_id"],
                StrategyEvent.state != "CLOSED",
            )
        )
        if other is not None:
            raise RiskRejected("SSR_INITIAL_SINGLE_ACTIVE_PAIR_LIMIT")
        if event and event.state != "CLOSED" and event.data["episode_id"] != note["episode_id"]:
            raise RiskRejected("SSR_PAIR_CAPITAL_STILL_OCCUPIED")
        if event and event.state == "CLOSED" and event.data["episode_id"] == note["episode_id"]:
            if not self.retryable_unsent(session, event) or note.get(
                "generation", 0
            ) <= event.data.get("generation", 0):
                raise RiskRejected("SSR_CLOSED_EPISODE_CANNOT_REENTER")
        qty = number(note["quantity"], positive=True)
        if quantity != qty:
            raise RiskRejected("SSR_EQUAL_FIXED_LEG_QUANTITY_REQUIRED")
        if intent.limit_price != number(note["leg_limits"][intent.instrument_id], probability=True):
            raise RiskRejected("SSR_INTENT_LIMIT_DISAGREES_WITH_BASKET")
        total = ZERO
        for inst in instruments:
            position = session.get(Position, (account.id, inst.id))
            if event and event.state != "CLOSED" and position and position.quantity == qty:
                completed = [
                    session.get(Order, key) for key in event.data.get("entry_order_ids", [])
                ]
                completed = [o for o in completed if o and o.instrument_id == inst.id]
                if (
                    len(completed) != 1
                    or completed[0].filled != qty
                    or completed[0].state != "FILLED"
                    or completed[0].request_state != "RESOLVED"
                    or position.cost is None
                ):
                    raise RiskRejected("SSR_COMPLETED_LEG_COST_NOT_CONFIRMED")
                total += position.cost
                continue
            depth = sweep(self.levels(inst, "asks"), qty)
            limit = number(note["leg_limits"][inst.id], probability=True)
            if depth is None or any(number(x["price"]) > limit for x in depth["fills"]):
                raise RiskRejected("SSR_ENTRY_DEPTH_OR_LIMIT_MISSING")
            total += qty * limit + qty * inst.fee_per_share
        if qty - total <= qty * number(note["min_margin_per_share"]):
            raise RiskRejected("SSR_NO_COST_ADJUSTED_NORMAL_EDGE")
        if total > number(proof["cash_budget"], positive=True):
            raise RiskRejected("SSR_PAIR_CASH_BUDGET_EXCEEDED")
        if total > number(
            proof.get("max_worst_case_cash_loss", proof["cash_budget"]), positive=True
        ):
            raise RiskRejected("SSR_FULL_COST_LOSS_BUDGET_EXCEEDED")
        if qty > number(proof.get("max_quantity_per_leg", "10"), positive=True):
            raise RiskRejected("SSR_INITIAL_QUANTITY_LIMIT")
        return instruments

    def calculate(self, session, account, intent):
        plan = super().calculate(session, account, intent)
        note = self.note(intent)
        if note["kind"] == "ENTRY":
            if intent.action != "INCREASE_TO_TARGET":
                raise RiskRejected("SSR_ENTRY_MUST_BUY_NO")
            self.entry(session, account, intent, plan.quantity)
        else:
            if note["kind"] == "EXIT":
                self.proof(session, intent)
            event = self.event(session, account.id, note["pair_id"])
            if (
                intent.action != "REDUCE_TO_TARGET"
                or event is None
                or event.state == "CLOSED"
                or intent.instrument_id not in event.data["instrument_ids"]
            ):
                raise RiskRejected("SSR_EXIT_OWNERSHIP_REQUIRED")
            if note["kind"] == "RISK_EXIT":
                self.recovery_plan(session, event, intent, plan.quantity)
        return plan

    def check_dispatch(self, session, account, intent, order):
        note = self.note(intent)
        event = self.event(session, account.id, note["pair_id"])
        if event is None or event.data["episode_id"] != note["episode_id"]:
            raise RiskRejected("SSR_BASKET_STATE_CHANGED")
        if note["kind"] == "ENTRY":
            self.entry(session, account, intent, order.quantity)
            if len(event.data.get("entry_order_ids", [])) != 2:
                raise RiskRejected("SSR_BOTH_LEGS_NOT_RESERVED")
        elif note["kind"] == "EXIT":
            plan = event.data.get("exit_plan", {})
            if (
                len(event.data.get("exit_order_ids", [])) != 2
                or str(intent.limit_price) != plan.get("leg_limits", {}).get(intent.instrument_id)
                or order.quantity != number(plan.get("quantity", "0"))
            ):
                raise RiskRejected("SSR_PROFIT_EXIT_PLAN_CHANGED")
            self.current_books(
                [session.get(Instrument, key) for key in event.data["instrument_ids"]]
            )
            inst = session.get(Instrument, intent.instrument_id)
            qty, _ = consume(
                [
                    level
                    for level in self.levels(inst, "bids")
                    if number(level["price"]) >= order.price
                ],
                order.quantity - order.filled,
                buy=False,
            )
            if qty != order.quantity - order.filled:
                raise RiskRejected("SSR_REMAINING_PROFIT_EXIT_DEPTH_MISSING")
        else:
            self.recovery_plan(session, event, intent, order.quantity - order.filled)
        for oid in event.data.get("entry_order_ids", []) + event.data.get("exit_order_ids", []):
            other = session.get(Order, oid)
            if (
                other
                and other.id != order.id
                and (
                    other.request_state in {"SENDING", "UNKNOWN"}
                    or other.cancel_state == "CANCEL_UNKNOWN"
                )
            ):
                raise RiskRejected("SSR_OTHER_LEG_UNKNOWN_KEEP_RESERVED")

    def on_reserved(self, session, account, intent, plan, order_id):
        note = self.note(intent)
        event = self.event(session, account.id, note["pair_id"])
        if note["kind"] == "ENTRY":
            proof, instruments = self.proof(session, intent)
            if event is None:
                event = StrategyEvent(
                    account_id=account.id,
                    strategy_id=IDENTITY[0],
                    strategy_version=IDENTITY[1],
                    event_key=note["pair_id"],
                    instrument_id=intent.instrument_id,
                    state="BUILDING",
                    state_group=note.get("state", "UNKNOWN"),
                    shock_group=note["pair_id"],
                    direction_bucket="PAIR_NO",
                    data={},
                )
                session.add(event)
            if event.state == "CLOSED" or not event.data:
                event.data = {
                    **note,
                    "instrument_ids": proof["instrument_ids"],
                    "entry_order_ids": [],
                    "exit_order_ids": [],
                }
                event.holding_started_at = None
            event.data = {
                **event.data,
                "entry_order_ids": [*event.data["entry_order_ids"], order_id],
            }
            event.state = "BUILDING"
            event.future_fee_reserve = number(note["quantity"]) * sum(
                (i.fee_per_share for i in instruments), ZERO
            )
        else:
            event.data = {
                **event.data,
                "exit_order_ids": [*event.data.get("exit_order_ids", []), order_id],
            }
            event.state = "EXIT_PENDING"

    def project(self, session, account, intent):
        note = self.note(intent)
        event = self.event(session, account.id, note["pair_id"])
        if event is None or event.data["episode_id"] != note["episode_id"]:
            return
        orders = [
            session.get(Order, key)
            for key in event.data["entry_order_ids"] + event.data["exit_order_ids"]
        ]
        positions = [
            session.get(Position, (account.id, key)) for key in event.data["instrument_ids"]
        ]
        qtys = [p.quantity if p else ZERO for p in positions]
        if any(
            o.request_state in {"SENDING", "UNKNOWN"} or o.cancel_state == "CANCEL_UNKNOWN"
            for o in orders
        ):
            event.state = "UNKNOWN_LEG"
        elif not any(qtys) and all(
            o.state in TERMINAL and o.request_state == "RESOLVED" for o in orders
        ):
            event.state, event.future_fee_reserve = "CLOSED", ZERO
        elif event.data["exit_order_ids"]:
            event.state = "EXIT_PENDING"
        elif (
            len(orders) == 2
            and all(o.state in TERMINAL and o.request_state == "RESOLVED" for o in orders)
            and qtys[0] == qtys[1] > 0
        ):
            event.state = "PAIRED_HELD"
        else:
            event.state = "UNPAIRED_HELD" if any(qtys) else "BUILDING"
        if any(qtys) and event.holding_started_at is None:
            event.holding_started_at = self.clock.now
        event.data = {
            **event.data,
            "owned_quantities": [str(q) for q in qtys],
            "capital_released": event.state == "CLOSED",
            "projected_at": self.clock.now.isoformat(),
        }

    def on_observed(self, session, account, order, observation):
        intent = Intent.model_validate(session.get(IntentRow, order.intent_id).payload)
        self.project(session, account, intent)

    def on_unsent_expired(self, session, account, intent, order):
        self.project(session, account, intent)

    def on_unsent_canceled(self, session, account, intent, order):
        self.project(session, account, intent)

    def reserve_pair(self, intents):
        if len(intents) != 2 or len({i.instrument_id for i in intents}) != 2:
            raise RiskRejected("SSR_TWO_LEG_BASKET_REQUIRED")
        left, right = map(self.note, intents)
        if left["kind"] == "RISK_EXIT":
            raise RiskRejected("SSR_RISK_EXIT_USES_REMAINING_OWNED_LEG")
        if (
            content_hash(left) != content_hash(right)
            or intents[0].account_id != intents[1].account_id
        ):
            raise RiskRejected("SSR_BASKET_SCOPE_MISMATCH")
        with self.store.transaction() as session:
            account = self.store.lock_account(session, intents[0].account_id)
            known = [session.get(IntentRow, i.intent_id) for i in intents]
            if any(known) and not all(known):
                raise RiskRejected("SSR_INCOMPLETE_LOCAL_BASKET_IDEMPOTENCY")
            if left["kind"] == "EXIT" and not all(known):
                event = self.event(session, account.id, left["pair_id"])
                capacity = self._exit_capacity(session, event, "0.60")
                qty = number(capacity["quantity"])
                planned_gain = (
                    sum((qty * i.limit_price for i in intents), ZERO)
                    - number(capacity["FIFO_cost"])
                    - number(capacity["exit_fees"])
                )
                required = max(
                    ZERO, qty - number(capacity["FIFO_cost"]) - number(capacity["exit_fees"])
                ) * Decimal("0.60")
                if planned_gain <= 0 or planned_gain < required:
                    raise RiskRejected("SSR_EXIT_LIMITS_DO_NOT_LOCK_PROPOSED_NET_GAIN")
                event.data = {
                    **event.data,
                    "exit_plan": {
                        **capacity,
                        "leg_limits": {i.instrument_id: str(i.limit_price) for i in intents},
                    },
                }
            orders = [self.reserve_locked(session, account, intent) for intent in intents]
            if any(oid is None for oid in orders):
                raise RiskRejected("SSR_BASKET_QUANTITY_CHANGED_REPLAN_REQUIRED")
            queue = CommandQueue(self.store, account.id, *IDENTITY)
            commands = [
                queue.publish_preallocated(session, intent, oid)
                for intent, oid in zip(intents, orders, strict=True)
            ]
            return {"orders": orders, "commands": commands, "venue_atomic": False}

    def exit_capacity(self, account_id, pair_id, *, take_fraction="0.60"):
        """Actual current bid capacity and FIFO cost; never a VWAP or proxy-trigger exit."""
        with self.store.transaction() as session:
            event = self.event(session, account_id, pair_id)
            return self._exit_capacity(session, event, take_fraction)

    def recovery_plan(self, session, event, intent, quantity):
        """A covered partial-leg sale with an explicit loss budget, after order closure."""
        if event is None or event.state not in {"UNPAIRED_HELD", "EXIT_PENDING"}:
            raise RiskRejected("SSR_UNPAIRED_RECOVERY_STATE_REQUIRED")
        entry_orders = [session.get(Order, key) for key in event.data["entry_order_ids"]]
        if any(
            o.state not in TERMINAL or o.request_state != "RESOLVED" or o.reserve_cash
            for o in entry_orders
        ):
            raise RiskRejected("SSR_ENTRY_CLOSURE_REQUIRED_BEFORE_RISK_EXIT")
        inst = session.get(Instrument, intent.instrument_id)
        self.current_books([inst])
        position = session.get(Position, (event.account_id, inst.id))
        if (
            position is None
            or position.cost is None
            or position.quantity < quantity
            or quantity <= 0
        ):
            raise RiskRejected("SSR_RECOVERY_COST_OR_QUANTITY_UNKNOWN")
        note = self.note(intent)
        loss_budget = number(note.get("max_recovery_loss", "0"))
        if loss_budget < 0 or loss_budget > number(event.data.get("max_recovery_loss", "0")):
            raise RiskRejected("SSR_RECOVERY_LOSS_BUDGET_CHANGED")
        # Prior losses are not recycled. Aggregate reserved exits cannot exceed this bound.
        prior = sum(
            (
                number(v)
                for k, v in event.data.get("recovery_loss_by_intent", {}).items()
                if k != intent.intent_id
            ),
            ZERO,
        )
        loss = max(
            ZERO, position.cost - quantity * intent.limit_price + quantity * inst.fee_per_share
        )
        if prior + loss > loss_budget:
            raise RiskRejected("SSR_RECOVERY_LOSS_BUDGET_EXCEEDED")
        got, _ = consume(
            [
                level
                for level in self.levels(inst, "bids")
                if number(level["price"]) >= intent.limit_price
            ],
            quantity,
            buy=False,
        )
        if got != quantity:
            raise RiskRejected("SSR_RECOVERY_EXTERNAL_DEPTH_MISSING")
        return loss

    def reserve_recovery(self, intent):
        if self.note(intent)["kind"] != "RISK_EXIT":
            raise RiskRejected("SSR_EXPLICIT_RISK_EXIT_REQUIRED")
        with self.store.transaction() as session:
            account = self.store.lock_account(session, intent.account_id)
            old = session.get(IntentRow, intent.intent_id)
            event = self.event(session, account.id, self.note(intent)["pair_id"])
            loss = (
                ZERO
                if old
                else self.recovery_plan(
                    session, event, intent, number(self.note(intent)["quantity"])
                )
            )
            oid = self.reserve_locked(session, account, intent)
            if oid is None:
                raise RiskRejected("SSR_RECOVERY_QUANTITY_CHANGED")
            if not old:
                losses = {
                    **event.data.get("recovery_loss_by_intent", {}),
                    intent.intent_id: str(loss),
                }
                event.data = {
                    **event.data,
                    "recovery_loss_by_intent": losses,
                    "recovery_loss_reserved": str(sum((number(v) for v in losses.values()), ZERO)),
                }
            command = CommandQueue(self.store, account.id, *IDENTITY).publish_preallocated(
                session, intent, oid
            )
            return {"order": oid, "command": command, "profit_realized": False}

    def _exit_capacity(self, session, event, take_fraction):
        account_id = event.account_id if event else None
        if event is None or event.state != "PAIRED_HELD":
            raise RiskRejected("SSR_COMPLETE_PAIR_REQUIRED_FOR_PROFIT_EXIT")
        insts = [session.get(Instrument, key) for key in event.data["instrument_ids"]]
        self.current_books(insts)
        positions = [session.get(Position, (account_id, i.id)) for i in insts]
        if (
            any(p is None or p.cost is None for p in positions)
            or positions[0].quantity != positions[1].quantity
        ):
            raise RiskRejected("SSR_FIFO_PAIR_COST_OR_QUANTITY_UNKNOWN")
        qty = positions[0].quantity
        proceeds, cost, fees = ZERO, ZERO, ZERO
        for inst, position in zip(insts, positions, strict=True):
            filled, value = consume(self.levels(inst, "bids"), qty, buy=False)
            if filled != qty:
                raise RiskRejected("SSR_EXIT_DEPTH_INSUFFICIENT")
            proceeds += value
            cost += position.cost
            fees += qty * inst.fee_per_share
        gain = proceeds - cost - fees
        conditional_remaining = qty - cost - fees
        if gain <= 0 or gain < max(ZERO, conditional_remaining) * number(take_fraction):
            raise RiskRejected("SSR_EARLY_PROFIT_EXIT_NOT_EXECUTABLE")
        return {
            "quantity": str(qty),
            "current_bid_proceeds": str(proceeds),
            "FIFO_cost": str(cost),
            "exit_fees": str(fees),
            "conditional_quote_gain": str(gain),
            "profit_realized": False,
        }
