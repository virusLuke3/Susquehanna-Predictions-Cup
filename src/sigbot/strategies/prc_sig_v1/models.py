"""Strict original-report, event, quote and SIG-only experiment inputs."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import timedelta
from decimal import Decimal
from typing import Literal

from pydantic import Field, model_validator

from sigbot.domain import AwareTime, Money, StrictModel, content_hash
from sigbot.strategies.shared import EventIdentity, Evidence


@dataclass(frozen=True)
class Policy:
    single_fraction: Decimal = Decimal("0.005")
    total_fraction: Decimal = Decimal("0.015")
    stop_fraction: Decimal = Decimal("0.0005")
    daily_fraction: Decimal = Decimal("0.02")
    cash_floor: Decimal = Decimal("0.97")
    max_events: int = 3
    max_per_state: int = 1
    max_per_report: int = 1
    max_per_sample: int = 1
    cooldown_seconds: int = 86400
    holding_seconds: int = 21600
    terminal_buffer_days: int = 7
    peak_loss: Decimal = Decimal("0.03")
    daily_loss: Decimal = Decimal("0.01")
    plan_seconds: int = 5


POLICY = Policy()


class PrcConfig(StrictModel):
    strategy_id: Literal["prc_sig"] = "prc_sig"
    strategy_version: Literal["1.0"] = "1.0"
    mode: Literal["OFFLINE"] = "OFFLINE"
    execution_venue: Literal["SIG"] = "SIG"
    live_authorization: None = None
    initial_capital: Money = Field(gt=0)
    parameters: dict[str, str | int] = Field(
        default_factory=lambda: {
            k: str(v) if isinstance(v, Decimal) else v for k, v in asdict(POLICY).items()
        }
    )

    @model_validator(mode="after")
    def frozen(self):
        expected = {k: str(v) if isinstance(v, Decimal) else v for k, v in asdict(POLICY).items()}
        if self.parameters != expected:
            raise ValueError(
                "PRC v1 risk/thresholds are frozen; register a new version to change them"
            )
        return self


class PollQuestion(StrictModel):
    question_id: str = Field(min_length=1)
    event: EventIdentity
    wording: str = Field(min_length=1)
    choices: list[str] = Field(min_length=2, max_length=2)
    population: str = Field(min_length=1)
    mode: str = Field(min_length=1)
    sampling_frame: str = Field(min_length=1)
    weighting: str = Field(min_length=1)
    sponsor: str = Field(min_length=1)
    sample_group: str = "UNKNOWN"
    sample_structure: str = Field(min_length=1)
    contrast: Money | None = Field(default=None, ge=-1, le=1)
    contrast_variance: Money | None = Field(default=None, gt=0)
    variance_kind: Literal[
        "CONTRAST_DESIGN", "CONTRAST_MODEL", "OPTION_VARIANCE", "REPORTED_MOE", "UNKNOWN"
    ] = "UNKNOWN"
    variance_evidence: str | None = None
    mapping_verified: bool = False

    def series_key(self, organization: str, synthetic: bool) -> str:
        return content_hash(
            [
                self.event.event_key(synthetic),
                organization,
                self.wording,
                self.choices,
                self.population,
                self.mode,
                self.sampling_frame,
                self.weighting,
                self.sponsor,
            ]
        )


class PollReport(StrictModel):
    organization: str = Field(min_length=1)
    original_report_id: str = Field(min_length=1)
    original: Evidence
    first_public_verified: bool = False
    original_verified: bool = False
    fieldwork_start: AwareTime
    fieldwork_end: AwareTime
    revision: str = Field(min_length=1)
    questions: list[PollQuestion]
    synthetic: bool
    withdrawn: bool = False

    @model_validator(mode="after")
    def dates(self):
        if self.fieldwork_end <= self.fieldwork_start:
            raise ValueError("poll fieldwork interval invalid")
        if len({q.question_id for q in self.questions}) != len(self.questions):
            raise ValueError("duplicate report question id")
        return self

    @property
    def release_id(self) -> str:
        # Mirrors, URLs and receipt times do not mint another release or reset t0.
        return ("SIM-RELEASE-" if self.synthetic else "RELEASE-") + content_hash(
            [self.organization.strip().casefold(), self.original_report_id.strip().casefold()]
        )[:24]

    @property
    def digest(self) -> str:
        return content_hash(
            [
                self.original.sha256,
                self.original.published_at,
                self.revision,
                self.withdrawn,
                self.fieldwork_start,
                self.fieldwork_end,
                [q.model_dump(mode="json") for q in self.questions],
            ]
        )


class PairReview(StrictModel):
    old_release_id: str
    new_release_id: str
    event_key: str
    old_report_digest: str
    new_report_digest: str
    comparability_verified: bool = False
    independence_verified: bool = False
    stable_house_effect_assumed: bool = False
    evidence: Evidence
    reviewer: str | None = None
    reviewed_at: AwareTime | None = None


class Level(StrictModel):
    price: Money = Field(gt=0, lt=1)
    quantity: Money = Field(gt=0)


class Book(StrictModel):
    bids: list[Level]
    asks: list[Level]
    source_at: AwareTime
    received_at: AwareTime
    verified_at: AwareTime
    version: str
    gap_free: bool = False
    own_orders_known: bool = False


class QuoteFrame(StrictModel):
    at: AwareTime
    received_at: AwareTime
    available_at: AwareTime
    bid: Money | None = Field(default=None, gt=0, lt=1)
    ask: Money | None = Field(default=None, gt=0, lt=1)
    gap_free: bool = False
    minute_complete: bool = False


class MarketReview(StrictModel):
    rules: Evidence
    mapping_hash: str | None = None
    approved: bool = False
    reviewer: str | None = None
    approved_at: AwareTime | None = None
    expires_at: AwareTime | None = None
    event_verified: bool = False
    binary_sides_verified: bool = False
    api_identity_verified: bool = False
    exchange_identity_verified: bool = False
    page_identity_verified: bool = False
    fee_verified: bool = False
    fee_evidence: Evidence | None = None
    buy_fee_bound: Money | None = Field(default=None, ge=0)
    sell_fee_bound: Money | None = Field(default=None, ge=0)


class Candidate(StrictModel):
    key: str
    event: EventIdentity
    page_id: str | None = None
    api_market_id: str
    exchange_id: str
    instrument_ids: dict[Literal["YES", "NO"], str]
    release_id: str
    rules_current_hash: str
    review: MarketReview
    books: dict[Literal["YES", "NO"], Book]
    history: dict[Literal["YES", "NO"], list[QuoteFrame]]
    tick: Money = Field(gt=0)
    lot: Money = Field(gt=0)
    minimum_quantity: Money = Field(gt=0)
    minimum_notional: Money = Field(default=Decimal("0"), ge=0)
    earliest_event_or_close: AwareTime | None = None
    close_verified: bool = False
    intervening_material_information: bool = True

    @property
    def mapping_hash(self) -> str:
        return content_hash(
            [
                self.event.model_dump(mode="json"),
                self.api_market_id,
                self.page_id,
                self.exchange_id,
                self.instrument_ids,
                self.review.binary_sides_verified,
            ]
        )


class Exposure(StrictModel):
    event_key: str
    state: str = "UNKNOWN"
    report_id: str = "UNKNOWN"
    sample_group: str = "UNKNOWN"
    amount: Money = Field(ge=0)
    active: bool = True


class AccountInput(StrictModel):
    initial: Money = Field(gt=0)
    equity: Money = Field(ge=0)
    cash: Money = Field(ge=0)
    high_water: Money = Field(gt=0)
    day_start_equity: Money = Field(gt=0)
    reserved: Money = Field(default=Decimal("0"), ge=0)
    daily_committed: Money = Field(default=Decimal("0"), ge=0)
    exposures: list[Exposure] = Field(default_factory=list)
    cooldowns: dict[str, AwareTime] = Field(default_factory=dict)
    reconciled: bool = False
    unknown_requests: bool = True


class Snapshot(StrictModel):
    strategy_id: Literal["prc_sig"] = "prc_sig"
    strategy_version: Literal["1.0"] = "1.0"
    synthetic: bool
    account_id: str
    tournament_id: str
    now: AwareTime
    account: AccountInput
    reports: list[PollReport] = Field(default_factory=list)
    comparisons: list[PairReview] = Field(default_factory=list)
    candidates: list[Candidate] = Field(default_factory=list)
    catalog_complete: bool = False
    catalog_markets: int = Field(default=0, ge=0)

    @model_validator(mode="after")
    def namespace(self):
        identifiers = [self.account_id, self.tournament_id]
        for candidate in self.candidates:
            identifiers += [
                candidate.key,
                candidate.api_market_id,
                candidate.exchange_id,
                *candidate.instrument_ids.values(),
            ]
        if any(x.startswith("SIM-") != self.synthetic for x in identifiers):
            raise ValueError("PRC real/synthetic namespace mismatch")
        if any(r.synthetic != self.synthetic for r in self.reports):
            raise ValueError("poll namespace mismatch")
        if len({c.key for c in self.candidates}) != len(self.candidates):
            raise ValueError("duplicate candidate key")
        return self

    def plan(self) -> dict:
        from sigbot.strategies.prc_sig_v1.core import allocate

        return allocate(self)


def deadline(report: PollReport):
    return (
        report.original.published_at + timedelta(minutes=16)
        if report.original.published_at
        else None
    )
