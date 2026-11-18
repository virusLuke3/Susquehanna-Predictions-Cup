"""Credential-free SSR proposals for the common account consumer.

Book refresh and venue writes belong to existing shared infrastructure. This
producer never creates a rule review or treats an account reservation as a fill.
"""

import json
from datetime import timedelta

from pydantic import Field

from sigbot.command_queue import CommandQueue
from sigbot.domain import TERMINAL, ZERO, Intent, Money, StrictModel, content_hash, utc_time
from sigbot.errors import RiskRejected, SigbotError
from sigbot.storage import Account, Instrument, IntentRow, Order, Position, StrategyEvaluation
from sigbot.strategies.semantic_sig_v1.core import number, sweep
from sigbot.strategies.ssr import IDENTITY


class Parameters(StrictModel):
    quantity: Money = Field(default="1", gt=0, le=10, validate_default=True)
    min_margin_per_share: Money = Field(default="0.010", gt=0, validate_default=True)
    exit_take_fraction: Money = Field(default="0.60", gt=0, le=1, validate_default=True)
    max_recovery_loss: Money = Field(default="0.10", ge=0, validate_default=True)
    cooldown_seconds: int = Field(default=900, ge=900, strict=True)
    observation_gap_seconds: int = Field(default=180, ge=1, le=180, strict=True)
    extra_fee_per_share: Money = Field(default="0", ge=0, le=0, validate_default=True)


class SsrProducer:
    def __init__(self, planner, account_id, parameters=None):
        self.planner, self.store, self.clock = planner, planner.store, planner.clock
        self.account_id = account_id
        self.parameters = parameters or Parameters()
        self.protocol_hash = content_hash(self.parameters.model_dump(mode="json"))

    def proposal(self, session, pair_id, insts, episode, kind, quantity, limits, *, generation=0):
        account = session.get(Account, self.account_id)
        proof = insts[0].mapping_evidence.get("ssr_pair_review", {})
        if not isinstance(proof, dict) or not proof.get("cash_budget"):
            raise RiskRejected("SSR_PAIR_REVIEW_OR_CASH_SCOPE_MISSING")
        note = {
            "kind": kind,
            "pair_id": pair_id,
            "episode_id": episode,
            "quantity": str(quantity),
            "review_hash": content_hash(proof),
            "leg_limits": {i.id: str(p) for i, p in zip(insts, limits, strict=True)},
            "min_margin_per_share": str(self.parameters.min_margin_per_share),
            "max_recovery_loss": str(self.parameters.max_recovery_loss),
            "protocol_hash": self.protocol_hash,
            "generation": generation,
        }
        stamp = self.clock.now
        result = []
        for inst, price in zip(insts, limits, strict=True):
            position = session.get(Position, (account.id, inst.id))
            held = position.quantity if position else ZERO
            result.append(
                Intent(
                    schema_version="1.0",
                    intent_id=("SIM-SSR-" if account.id.startswith("SIM-") else "SIG-SSR-")
                    + content_hash([episode, generation, kind, inst.id, str(held)]),
                    synthetic=account.id.startswith("SIM-"),
                    account_id=account.id,
                    tournament_id=account.tournament,
                    instrument_id=inst.id,
                    outcome_side="NO",
                    action="INCREASE_TO_TARGET" if kind == "ENTRY" else "REDUCE_TO_TARGET",
                    target_quantity=quantity if kind == "ENTRY" else ZERO,
                    limit_price=price,
                    max_order_notional=number(proof["cash_budget"]),
                    created_at=stamp,
                    expires_at=stamp + timedelta(seconds=180),
                    rules_hash=inst.rules_hash,
                    mapping_hash=inst.mapping_hash,
                    strategy_id=IDENTITY[0],
                    strategy_version=IDENTITY[1],
                    note=json.dumps(note),
                )
            )
        return result

    def episode(self, session, pair_id, edge):
        """A discrete scan latch, not proof that prices stayed quiet between scans."""
        key = (self.account_id, *IDENTITY, self.protocol_hash, pair_id)
        row = session.get(StrategyEvaluation, key)
        now = self.clock.now
        if row is None:
            row = StrategyEvaluation(
                account_id=self.account_id,
                strategy_id=IDENTITY[0],
                strategy_version=IDENTITY[1],
                release_id=self.protocol_hash,
                event_key=pair_id,
                evaluated_at=now,
                state="WATCHING",
                data={},
            )
            session.add(row)
        data = dict(row.data)
        continuous = (
            data.get("last_observed")
            and 0
            <= (now - utc_time(data["last_observed"])).total_seconds()
            <= self.parameters.observation_gap_seconds
        )
        if edge is None:
            data.pop("quiet_since", None)
        elif not edge:
            if not continuous or not data.get("quiet_since"):
                data["quiet_since"] = now.isoformat()
            if (
                now - utc_time(data["quiet_since"])
            ).total_seconds() >= self.parameters.cooldown_seconds:
                data.pop("episode_id", None)
                data.pop("submitted", None)
        else:
            data.pop("quiet_since", None)
            if not data.get("episode_id"):
                data["episode_id"] = content_hash([self.protocol_hash, pair_id, now.isoformat()])
        data["last_observed"] = now.isoformat()
        row.data, row.evaluated_at = data, now
        row.state = "UNAVAILABLE" if edge is None else "EDGE" if edge else "COOLDOWN"
        return data

    def cycle(self, pair_id, instrument_ids, *, execute=False):
        """One bounded evaluation; executable scopes still require gateway authority."""
        output = {"pair_id": pair_id, "execute": execute, "realized_profit": None}
        recovery = False
        cancellations = []
        try:
            with self.store.transaction() as session:
                account = self.store.lock_account(session, self.account_id)
                insts = [session.get(Instrument, key) for key in instrument_ids]
                if len(insts) != 2 or any(i is None for i in insts):
                    raise RiskRejected("SSR_REVIEWED_NO_INSTRUMENTS_MISSING")
                self.planner.current_books(insts)
                event = self.planner.event(session, self.account_id, pair_id)
                if event and event.state != "CLOSED":
                    entry = session.get(
                        IntentRow, session.get(Order, event.data["entry_order_ids"][0]).intent_id
                    )
                    self.planner.project(session, account, Intent.model_validate(entry.payload))
                if event and event.state != "CLOSED":
                    output.update(state=event.state, capital_released=False)
                    if event.state == "PAIRED_HELD":
                        capacity = self.planner._exit_capacity(
                            session, event, self.parameters.exit_take_fraction
                        )
                        quantity = number(capacity["quantity"])
                        limits = []
                        for inst in insts:
                            levels = self.planner.levels(inst, "bids")
                            # Worst executable price, not an average-price limit.
                            remaining, last = quantity, None
                            for level in levels:
                                take = min(remaining, number(level["quantity"]))
                                if take:
                                    remaining -= take
                                    last = number(level["price"])
                                if not remaining:
                                    break
                            if remaining or last is None:
                                raise RiskRejected("SSR_FULL_EXIT_DEPTH_REQUIRED")
                            limits.append(last)
                        proposals = self.proposal(
                            session,
                            pair_id,
                            insts,
                            event.data["episode_id"],
                            "EXIT",
                            quantity,
                            limits,
                        )
                        output["quoted_exit"] = capacity
                    elif event.state in {"BUILDING", "UNPAIRED_HELD"}:
                        orders = [session.get(Order, key) for key in event.data["entry_order_ids"]]
                        open_orders = [
                            o
                            for o in orders
                            if o.state not in TERMINAL or o.request_state != "RESOLVED"
                        ]
                        if open_orders:
                            for order in open_orders:
                                payload = session.get(IntentRow, order.intent_id).payload
                                if self.clock.now >= utc_time(payload["expires_at"]):
                                    cancellations.append(order.id)
                            proposals = []
                        else:
                            owned = [
                                (inst, session.get(Position, (account.id, inst.id)))
                                for inst in insts
                            ]
                            owned = [(i, p) for i, p in owned if p is not None and p.quantity > 0]
                            if len(owned) != 1:
                                raise RiskRejected(
                                    "SSR_ASYMMETRIC_TWO_LEG_POSITION_REQUIRES_REVIEW"
                                )
                            inst, position = owned[0]
                            levels = self.planner.levels(inst, "bids")
                            depth = sorted(levels, key=lambda x: number(x["price"]), reverse=True)
                            remaining, limit = position.quantity, None
                            for level in depth:
                                take = min(remaining, number(level["quantity"]))
                                if take:
                                    remaining -= take
                                    limit = number(level["price"])
                                if not remaining:
                                    break
                            if remaining or limit is None:
                                raise RiskRejected("SSR_UNPAIRED_EXIT_DEPTH_MISSING")
                            proposals = self.proposal(
                                session,
                                pair_id,
                                [inst],
                                event.data["episode_id"],
                                "RISK_EXIT",
                                position.quantity,
                                [limit],
                            )
                            recovery = True
                    else:
                        return output
                else:
                    quantity = self.parameters.quantity
                    quoted = [sweep(self.planner.levels(i, "asks"), quantity) for i in insts]
                    if any(q is None for q in quoted):
                        raise RiskRejected("SSR_PAIR_ENTRY_DEPTH_INSUFFICIENT")
                    limits = [number(q["fills"][-1]["price"]) for q in quoted]
                    edge = (
                        quantity - quantity * sum(limits, ZERO)
                        > quantity * self.parameters.min_margin_per_share
                    )
                    tracking = self.episode(session, pair_id, edge)
                    if (
                        edge
                        and tracking.get("submitted")
                        and self.planner.retryable_unsent(session, event)
                    ):
                        tracking = {
                            **tracking,
                            "submitted": False,
                            "generation": tracking.get("generation", 0) + 1,
                        }
                        row = session.get(
                            StrategyEvaluation,
                            (self.account_id, *IDENTITY, self.protocol_hash, pair_id),
                        )
                        row.data = tracking
                    if not edge or tracking.get("submitted"):
                        return {**output, "state": "NO_NEW_EPISODE"}
                    proposals = self.proposal(
                        session,
                        pair_id,
                        insts,
                        tracking["episode_id"],
                        "ENTRY",
                        quantity,
                        limits,
                        generation=tracking.get("generation", 0),
                    )
                    self.planner.proof(session, proposals[0])
                # Full planner checks, including the current account and asset ownership.
                if not execute:
                    for intent in proposals:
                        self.planner.calculate(session, account, intent)
                if proposals:
                    output["state"] = "VALIDATED_PROPOSAL"
                elif cancellations:
                    output["state"] = "ENTRY_CLOSURE_PENDING"
            if cancellations and execute:
                queue = CommandQueue(self.store, self.account_id, *IDENTITY)
                output["cancel_commands"] = [
                    queue.cancel(key, self.clock.now, reason="SSR_PARTIAL_ENTRY_EXPIRED")
                    for key in cancellations
                ]
            if not proposals:
                return output
            if execute:
                output["allocation"] = (
                    self.planner.reserve_recovery(proposals[0])
                    if recovery
                    else self.planner.reserve_pair(proposals)
                )
                output["state"] = "QUEUED_FOR_COMMON_ACCOUNT_CONSUMER"
                if proposals[0].action == "INCREASE_TO_TARGET":
                    with self.store.transaction() as session:
                        self.store.lock_account(session, self.account_id)
                        row = session.get(
                            StrategyEvaluation,
                            (self.account_id, *IDENTITY, self.protocol_hash, pair_id),
                        )
                        row.data = {**row.data, "submitted": True}
            return output
        except SigbotError as exc:
            with self.store.transaction() as session:
                self.store.lock_account(session, self.account_id)
                self.episode(session, pair_id, None)
            return {**output, "state": "BLOCKED", "code": exc.code}
