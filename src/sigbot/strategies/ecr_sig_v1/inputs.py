"""Normalized inputs and explicit review evidence. No network or credentials."""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator

from sigbot.domain import AwareTime, Money, StrictModel, content_hash
from sigbot.strategies.ecr_sig_v1 import core
from sigbot.strategies.registry import ECR
from sigbot.strategies.shared import EventIdentity, Evidence


def epoch(value: datetime) -> int:
    return int(value.timestamp())


def policy_parameters() -> dict[str, str | int]:
    return json.loads(json.dumps(asdict(core.DEFAULT_POLICY), default=str))


class EcrConfig(StrictModel):
    strategy_id: Literal["ecr_sig"] = "ecr_sig"
    strategy_version: Literal["1.0"] = "1.0"
    mode: Literal["OFFLINE"] = "OFFLINE"
    execution_venue: Literal["SIG"] = "SIG"
    external_venues: Literal["READ_ONLY"] = "READ_ONLY"
    live_authorization: None = None
    plan_ttl_seconds: Literal[5] = 5
    initial_capital: Money = Field(gt=0)
    parameters: dict[str, str | int] = Field(default_factory=policy_parameters)

    @model_validator(mode="after")
    def frozen_experiment(self):
        if self.parameters != policy_parameters():
            raise ValueError(
                "ECR v1 parameters are frozen; changing them requires a new registered version"
            )
        return self


class PayoffDefinition(StrictModel):
    predicate_kind: Literal["PARTY_WIN", "CANDIDATE_WIN"]
    predicate_subject: str = Field(min_length=1)
    yes_meaning: str = Field(min_length=1)
    no_meaning: str = Field(min_length=1)
    payout_if_yes: Money = Field(ge=1, le=1)
    payout_if_no: Money = Field(ge=0, le=0)
    na_rule: str = Field(min_length=1)
    replacement_rule: str = Field(min_length=1)
    dispute_and_delay_rule: str = Field(min_length=1)
    resolution_source: str = Field(min_length=1)


class MappingReview(StrictModel):
    operator: str = Field(min_length=1)
    external_contract_id: str = Field(min_length=1)
    event: EventIdentity
    # Includes YES predicate, NO predicate and exception semantics; never match by title.
    payoff_definition: PayoffDefinition
    rules: Evidence
    approved: bool = False
    reviewer: str | None = None
    approved_at: AwareTime | None = None
    expires_at: AwareTime | None = None
    approved_sig_rules_hash: str | None = None


class Review(StrictModel):
    rules: Evidence
    payoff_definition: PayoffDefinition
    approved: bool = False
    reviewer: str | None = None
    approved_at: AwareTime | None = None
    expires_at: AwareTime | None = None
    mappings: list[MappingReview] = Field(default_factory=list)
    fee_verified: bool = False
    fee_evidence: Evidence | None = None
    empirical_fee_file: str | None = None
    buy_fee_bound: Money | None = Field(default=None, ge=0)
    sell_fee_bound: Money | None = Field(default=None, ge=0)

    def fees_ready(self, now: datetime, synthetic: bool) -> bool:
        if self.empirical_fee_file:
            if synthetic:
                return False
            from sigbot.strategies.mm_sig_v1.live.fees import EmpiricalFee, load_fee

            fee = load_fee(self.empirical_fee_file)
            if not isinstance(fee, EmpiricalFee):
                return False
            fee.check(False)
            report = json.loads(Path(fee.evidence_path).read_text())
            return bool(
                datetime.fromisoformat(report["updated_at"]) <= now
                and self.buy_fee_bound == self.sell_fee_bound == fee.per_share
            )
        return bool(
            self.fee_verified
            and self.fee_evidence is not None
            and self.fee_evidence.kind == ("SYNTHETIC" if synthetic else "OFFICIAL")
            and self.fee_evidence.available_at <= now
            and self.buy_fee_bound is not None
            and self.sell_fee_bound is not None
        )

    @property
    def mapping_hash(self) -> str:
        return content_hash([m.model_dump(mode="json") for m in self.mappings])


class LevelInput(StrictModel):
    price: Money = Field(gt=0, lt=1)
    size: Money = Field(gt=0)


class BookInput(StrictModel):
    outcome_side: Literal["YES", "NO"] = "YES"
    bids: list[LevelInput]
    asks: list[LevelInput]
    source_at: AwareTime | None
    received_at: AwareTime
    verified_at: AwareTime
    version: str = Field(min_length=1)
    gap_free: bool = False

    @model_validator(mode="after")
    def check_time(self):
        if not self.received_at <= self.verified_at or (
            self.source_at is not None and self.source_at > self.received_at
        ):
            raise ValueError("book time order invalid")
        return self

    def book(self) -> core.Book:
        return core.Book(
            tuple(core.Level(x.price, x.size) for x in self.bids),
            tuple(core.Level(x.price, x.size) for x in self.asks),
            epoch(self.verified_at),
            self.gap_free,
        )


class ExternalBook(StrictModel):
    operator: str = Field(min_length=1)
    contract_id: str = Field(min_length=1)
    book: BookInput


class QuoteInput(StrictModel):
    operator: str = Field(min_length=1)
    bid: Money = Field(gt=0, lt=1)
    ask: Money = Field(gt=0, lt=1)


class MinuteInput(StrictModel):
    outcome_side: Literal["YES", "NO"] = "YES"
    at: AwareTime
    received_at: AwareTime
    available_at: AwareTime
    sig_bid: Money = Field(gt=0, lt=1)
    sig_ask: Money = Field(gt=0, lt=1)
    external: list[QuoteInput]
    gap_free: bool = False

    def observation(self, now: datetime) -> core.Observation:
        if not self.at <= self.received_at <= self.available_at <= now:
            raise core.Reject("observation not available at decision time")
        return core.Observation(
            epoch(self.at),
            self.sig_bid,
            self.sig_ask,
            tuple((x.operator, x.bid, x.ask) for x in self.external),
            self.gap_free,
        )


class Catalyst(StrictModel):
    event: EventIdentity
    evidence: Evidence
    verified: bool = False


class CandidateInput(StrictModel):
    key: str = Field(min_length=1)
    instrument_id: str = Field(min_length=1)
    exchange_id: str = Field(min_length=1)
    market_id: str = Field(min_length=1)
    event: EventIdentity
    side: Literal["YES", "NO"]
    shock_group: str | None = None
    direction_bucket: str | None = None
    rules_current_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    review: Review
    sig_book: BookInput
    external_books: list[ExternalBook]
    baseline: MinuteInput
    history: list[MinuteInput]
    tick: Money = Field(gt=0)
    lot: Money = Field(gt=0)
    minimum_quantity: Money = Field(gt=0)
    tradable_until: AwareTime
    catalyst: Catalyst | None = None
    last_event_exit_at: AwareTime | None = None

    def candidate(self, now: datetime, synthetic: bool) -> core.Candidate:
        operators = {"SIM-VENUE-A", "SIM-VENUE-B"} if synthetic else {"POLYMARKET", "KALSHI"}
        if any(book.operator not in operators for book in self.external_books):
            raise core.Reject("unknown external operator")
        if (
            self.sig_book.outcome_side != self.side
            or any(x.book.outcome_side != self.side for x in self.external_books)
            or any(x.outcome_side != self.side for x in [self.baseline, *self.history])
        ):
            raise core.Reject("quote outcome side mismatch")
        kind = "SYNTHETIC" if synthetic else "OFFICIAL"

        def approved(review) -> bool:
            return bool(
                review.approved
                and review.reviewer
                and review.approved_at is not None
                and review.expires_at is not None
                and review.rules.kind == kind
                and review.rules.available_at <= review.approved_at <= now < review.expires_at
            )

        r = self.review
        rules_ok = approved(r)
        mapping_ok = len(r.mappings) == len(self.external_books) and bool(r.mappings)
        for book in self.external_books:
            matches = [
                m
                for m in r.mappings
                if (m.operator, m.external_contract_id) == (book.operator, book.contract_id)
            ]
            mapping_ok = (
                mapping_ok
                and len(matches) == 1
                and all(
                    approved(m)
                    and m.event == self.event
                    and m.payoff_definition == r.payoff_definition
                    and m.approved_sig_rules_hash == self.rules_current_hash
                    for m in matches
                )
            )
        # Operational readiness includes an explicit empirical estimate;
        # Review.fee_verified remains false for that model.
        fee_ok = r.fees_ready(now, synthetic)
        catalyst_ok = bool(
            self.catalyst
            and self.catalyst.verified
            and self.catalyst.event == self.event
            and self.catalyst.evidence.kind == ("SYNTHETIC" if synthetic else "ORIGINAL_PUBLIC")
            and self.catalyst.evidence.published_at is not None
            and self.catalyst.evidence.available_at <= now
        )
        if (
            any(
                not 0 <= (now - b.book.received_at).total_seconds() <= 5
                for b in self.external_books
            )
            or not 0 <= (now - self.sig_book.received_at).total_seconds() <= 5
        ):
            raise core.Reject("book read stale or future")
        if (
            any(b.book.verified_at > now for b in self.external_books)
            or self.sig_book.verified_at > now
        ):
            raise core.Reject("future book verification")
        times = [x.at for x in self.history]
        if any(a >= b for a, b in zip(times, times[1:], strict=False)):
            raise core.Reject("history duplicate or out of order")
        if self.history and not 0 <= (now - self.history[-1].at).total_seconds() <= 10:
            raise core.Reject("stale signal window")
        if self.last_event_exit_at and (now - self.last_event_exit_at).total_seconds() < 21600:
            raise core.Reject("event cooldown")
        if not self.sig_book.gap_free:
            raise core.Reject("book version gap")
        return core.Candidate(
            key=self.key,
            event_key=self.event.event_key(synthetic),
            state=self.event.state,
            shock_group=self.shock_group or "UNKNOWN",
            direction_bucket=self.direction_bucket or "UNKNOWN",
            office=self.event.office,
            side=self.side,
            sig_book=self.sig_book.book(),
            external_books=tuple((x.operator, x.book.book()) for x in self.external_books),
            baseline=self.baseline.observation(now),
            history=tuple(x.observation(now) for x in self.history),
            rules_reviewed=rules_ok,
            mapping_reviewed=mapping_ok,
            rules_unchanged=self.rules_current_hash == r.rules.sha256,
            fee_verified=fee_ok,
            buy_fee_bound=r.buy_fee_bound or core.ZERO,
            sell_fee_bound=r.sell_fee_bound or core.ZERO,
            lot=self.lot,
            min_qty=self.minimum_quantity,
            price_tick=self.tick,
            catalyst_verified=catalyst_ok,
            catalyst_at=epoch(self.catalyst.evidence.published_at)
            if catalyst_ok and self.catalyst and self.catalyst.evidence.published_at
            else None,
            catalyst_event_key=self.catalyst.event.event_key(synthetic) if self.catalyst else None,
            last_event_exit_at=epoch(self.last_event_exit_at) if self.last_event_exit_at else None,
            tradable_until=epoch(self.tradable_until),
        )


class AccountInput(StrictModel):
    initial: Money = Field(gt=0)
    cash: Money = Field(ge=0)
    equity: Money = Field(ge=0)
    high_water: Money = Field(gt=0)
    day_start_equity: Money = Field(gt=0)
    reserved_and_future_fees: Money = Field(default=Decimal("0"), ge=0)
    total_exposure: Money = Field(default=Decimal("0"), ge=0)
    race_exposure: dict[str, Money] = Field(default_factory=dict)
    state_exposure: dict[str, Money] = Field(default_factory=dict)
    shock_exposure: dict[str, Money] = Field(default_factory=dict)
    direction_exposure: dict[str, Money] = Field(default_factory=dict)
    occupied_events: set[str] = Field(default_factory=set)
    reconciled: bool = False
    unknown_writes: bool = True

    def account(self) -> core.Account:
        return core.Account(**self.model_dump())


class CandidateReviewInput(StrictModel):
    """Operator facts only. No dummy books or invented pre-start history."""

    key: str = Field(min_length=1)
    instrument_id: str = Field(min_length=1)
    exchange_id: str = Field(min_length=1)
    market_id: str = Field(min_length=1)
    event: EventIdentity
    side: Literal["YES", "NO"]
    shock_group: str | None = None
    direction_bucket: str | None = None
    rules_current_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    review: Review
    tick: Money = Field(gt=0)
    lot: Money = Field(gt=0)
    minimum_quantity: Money = Field(gt=0)
    tradable_until: AwareTime
    catalyst: Catalyst | None = None


class RuntimeReviews(StrictModel):
    strategy_id: Literal["ecr_sig"] = "ecr_sig"
    strategy_version: Literal["1.0"] = "1.0"
    synthetic: Literal[False] = False
    account_id: str = Field(min_length=1)
    tournament_id: str = Field(min_length=1)
    account: AccountInput
    candidates: list[CandidateReviewInput]

    @model_validator(mode="after")
    def scope(self):
        refs = [self.account_id, self.tournament_id]
        refs.extend(
            x for c in self.candidates for x in (c.key, c.instrument_id, c.exchange_id, c.market_id)
        )
        if any(x.startswith("SIM-") for x in refs) or len({c.key for c in self.candidates}) != len(
            self.candidates
        ):
            raise ValueError("real unique review identities required")
        return self

    @classmethod
    def from_snapshot(cls, snapshot):
        return cls(
            account_id=snapshot.account_id,
            tournament_id=snapshot.tournament_id,
            account=snapshot.account.model_copy(deep=True),
            candidates=[
                CandidateReviewInput.model_validate(
                    c.model_dump(
                        exclude={
                            "sig_book",
                            "external_books",
                            "baseline",
                            "history",
                            "last_event_exit_at",
                        }
                    )
                )
                for c in snapshot.candidates
            ],
        )


class Snapshot(StrictModel):
    strategy_id: Literal["ecr_sig"] = "ecr_sig"
    strategy_version: Literal["1.0"] = "1.0"
    synthetic: bool
    account_id: str = Field(min_length=1)
    tournament_id: str = Field(min_length=1)
    now: AwareTime
    account: AccountInput
    candidates: list[CandidateInput]

    @model_validator(mode="after")
    def namespace(self):
        refs = [self.account_id, self.tournament_id]
        refs.extend(
            ref
            for c in self.candidates
            for ref in (c.key, c.instrument_id, c.exchange_id, c.market_id)
        )
        if any(x.startswith("SIM-") != self.synthetic for x in refs):
            raise ValueError("strategy input namespace mismatch")
        if len({c.key for c in self.candidates}) != len(self.candidates):
            raise ValueError("duplicate candidate key")
        operators = {x.operator for c in self.candidates for x in c.external_books}
        if not operators <= (
            {"SIM-VENUE-A", "SIM-VENUE-B"} if self.synthetic else {"POLYMARKET", "KALSHI"}
        ):
            raise ValueError("operator identity must be canonical, not an API alias")
        return self

    def plan(self) -> dict:
        account = self.account.account()
        candidates, rejects = [], {}
        for c in self.candidates:
            try:
                candidates.append(c.candidate(self.now, self.synthetic))
            except core.Reject as exc:
                rejects[c.key] = str(exc)
        plans, denied = core.allocate(candidates, account, epoch(self.now))
        rejects.update(denied)
        return {
            "strategy_id": ECR.strategy_id,
            "strategy_version": ECR.version,
            "layer": "MOCK" if self.synthetic else "LOCAL_DATA_PLANNING",
            "synthetic": self.synthetic,
            "plans": [asdict(p) for p in plans],
            "rejected": rejects,
            "reserved": str(account.reserved_and_future_fees),
            "cash_after_all_reserved_spent": str(account.cash - account.reserved_and_future_fees),
            "expected_pnl": None,
            "authorized_for_live": False,
            "network_calls": 0,
            "real_orders": 0,
        }


def normalize_binary_book(book: BookInput, side: Literal["YES", "NO"]) -> BookInput:
    """For a reviewed unit binary payoff only; complement AND swap the book sides."""
    if book.outcome_side == side:
        return book.model_copy(deep=True)
    return book.model_copy(
        update={
            "outcome_side": side,
            "bids": [LevelInput(price=Decimal("1") - x.price, size=x.size) for x in book.asks],
            "asks": [LevelInput(price=Decimal("1") - x.price, size=x.size) for x in book.bids],
        },
        deep=True,
    )
