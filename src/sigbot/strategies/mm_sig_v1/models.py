from __future__ import annotations

from typing import Literal

from pydantic import Field, model_validator

from sigbot.domain import AwareTime, Money, StrictModel


class MmConfig(StrictModel):
    strategy_id: Literal["mm_sig"] = "mm_sig"
    strategy_version: Literal["1.0"] = "1.0"
    mode: Literal["OFFLINE"] = "OFFLINE"
    initial_capital: Money = Field(default="1000", gt=0, validate_default=True)
    quote_quantity: Money = Field(default="10", gt=0, le=10, validate_default=True)
    max_inventory: Money = Field(default="20", gt=0, le=20, validate_default=True)
    max_order_cost: Money = Field(default="10", gt=0, le=10, validate_default=True)
    max_event_cost: Money = Field(default="10", gt=0, le=10, validate_default=True)
    max_total_cost: Money = Field(default="30", gt=0, le=30, validate_default=True)
    max_events: int = Field(default=3, ge=1, le=3)
    min_net_spread: Money = Field(default="0.02", ge="0.02", validate_default=True)
    exit_cost_buffer: Money = Field(default="0.005", ge=0, validate_default=True)
    improvement_ticks: int = Field(default=0, ge=0, le=1)
    inventory_skew_ticks: int = Field(default=1, ge=0, le=5)
    quote_seconds: int = Field(default=5, ge=1, le=5)
    book_seconds: int = Field(default=5, ge=1, le=5)
    max_inventory_seconds: int = Field(default=1800, ge=1, le=1800)
    min_recent_trades: int = Field(default=1, ge=1)

    @model_validator(mode="after")
    def limits(self):
        if self.quote_quantity > self.max_inventory or self.max_event_cost > self.max_total_cost:
            raise ValueError("inconsistent MM risk limits")
        return self


class Level(StrictModel):
    price: Money = Field(gt=0, lt=1)
    quantity: Money = Field(gt=0)
    own_quantity: Money = Field(default="0", ge=0, validate_default=True)

    @model_validator(mode="after")
    def own_bound(self):
        if self.own_quantity > self.quantity:
            raise ValueError("own quantity exceeds aggregate level")
        return self


class Book(StrictModel):
    received_at: AwareTime
    source_at: AwareTime
    validated_at: AwareTime | None = None
    continuity_verified: bool = False
    quality: Literal["HEALTHY", "GAP", "STALE", "UNKNOWN"]
    own_orders_known: bool
    sequence: int = Field(ge=0)
    bids: list[Level]
    asks: list[Level]


class Candidate(StrictModel):
    event_key: str = Field(min_length=1)
    market_id: str = Field(min_length=1)
    exchange_id: str = Field(min_length=1)
    instrument_id: str = Field(min_length=1)
    outcome: Literal["YES"] = "YES"
    rules_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    mapping_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    reviewed: bool
    review_until: AwareTime
    status: Literal["OPEN", "CLOSED", "UNKNOWN"]
    tick: Money = Field(gt=0)
    step: Money = Field(gt=0)
    fee_per_share: Money | None = Field(default=None, ge=0)
    recent_trade_count: int | None = Field(default=None, ge=0)
    activity_window_complete: bool
    activity_until: AwareTime
    book: Book


class Inventory(StrictModel):
    quantity: Money = Field(ge=0)
    cost: Money = Field(ge=0)
    first_fill_at: AwareTime | None = None


class AccountInput(StrictModel):
    initial: Money = Field(gt=0)
    cash: Money = Field(ge=0)
    reserved: Money = Field(ge=0)
    total_cost: Money = Field(ge=0)
    reconciled: bool
    unknown_orders: bool
    inventory: dict[str, Inventory] = Field(default_factory=dict)


class Snapshot(StrictModel):
    synthetic: bool
    account_id: str
    tournament_id: str
    now: AwareTime
    starts_at: AwareTime
    ends_at: AwareTime
    account: AccountInput
    candidates: list[Candidate]

    @model_validator(mode="after")
    def namespace(self):
        ids = [self.account_id, self.tournament_id]
        for c in self.candidates:
            ids += [c.event_key, c.market_id, c.exchange_id, c.instrument_id]
        if self.synthetic != all(x.startswith("SIM-") for x in ids) or (
            not self.synthetic and any(x.startswith("SIM-") for x in ids)
        ):
            raise ValueError("MM namespace mismatch")
        if self.starts_at >= self.ends_at:
            raise ValueError("invalid trading interval")
        if len({c.instrument_id for c in self.candidates}) != len(self.candidates):
            raise ValueError("duplicate MM instrument")
        return self
