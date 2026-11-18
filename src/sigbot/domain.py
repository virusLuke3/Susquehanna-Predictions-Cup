from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from typing import Annotated, Any

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, PlainSerializer, model_validator

from sigbot.errors import SchemaMismatch

ZERO = Decimal("0")
UTC = timezone.utc


def decimal_value(value: Any) -> Decimal:
    if not isinstance(value, (str, Decimal)):
        raise ValueError("Decimal requires a decimal string; float/int/bool forbidden")
    if isinstance(value, str) and not re.fullmatch(r"-?(0|[1-9][0-9]*)(\.[0-9]+)?", value):
        raise ValueError("invalid decimal string")
    result = Decimal(value)
    exponent = (
        result.normalize().as_tuple().exponent if result.is_finite() else result.as_tuple().exponent
    )
    if (
        not result.is_finite()
        or abs(result) >= Decimal("1e18")
        or not isinstance(exponent, int)
        or exponent < -10
    ):
        raise ValueError("nonfinite or precision overflow")
    return result


Money = Annotated[
    Decimal,
    BeforeValidator(decimal_value),
    PlainSerializer(lambda value: format(value, "f"), return_type=str, when_used="json"),
]


def utc_time(value: Any) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value
    if not isinstance(dt, datetime) or dt.tzinfo is None or dt.utcoffset() is None:
        raise ValueError("timezone required")
    return dt.astimezone(UTC)


AwareTime = Annotated[datetime, BeforeValidator(utc_time)]


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)


def content_hash(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True, strict=True)


class Mode(str, Enum):
    OFFLINE = "OFFLINE"
    READ_ONLY = "READ_ONLY"
    SHADOW = "SHADOW"
    LIVE_CANARY = "LIVE_CANARY"
    LIVE_LIMITED = "LIVE_LIMITED"
    SETTLEMENT_ONLY = "SETTLEMENT_ONLY"


class Intent(StrictModel):
    schema_version: str = Field(pattern=r"^1\.0$")
    intent_id: str = Field(min_length=1)
    synthetic: bool
    account_id: str = Field(min_length=1)
    tournament_id: str = Field(min_length=1)
    instrument_id: str = Field(min_length=1)
    outcome_side: str = Field(pattern="^(YES|NO)$")
    action: str = Field(pattern="^(INCREASE_TO_TARGET|REDUCE_TO_TARGET)$")
    target_quantity: Money = Field(ge=0)
    limit_price: Money = Field(gt=0, lt=1)
    max_order_notional: Money = Field(gt=0)
    created_at: AwareTime
    expires_at: AwareTime
    rules_hash: str = Field(pattern="^[0-9a-f]{64}$")
    mapping_hash: str = Field(pattern="^[0-9a-f]{64}$")
    strategy_id: str = Field(min_length=1)
    strategy_version: str = Field(min_length=1)
    note: str | None = None

    @model_validator(mode="after")
    def validate_scope(self) -> Intent:
        if self.expires_at <= self.created_at:
            raise ValueError("expiry must follow creation")
        refs = [self.account_id, self.tournament_id, self.instrument_id]
        if self.synthetic != all(x.startswith("SIM-") for x in refs):
            raise ValueError("synthetic namespace mismatch")
        if not self.synthetic and any(x.startswith("SIM-") for x in refs):
            raise ValueError("SIM forbidden in real namespace")
        return self

    @property
    def digest(self) -> str:
        return content_hash(self.model_dump(mode="json"))


@dataclass
class Clock:
    now: datetime = field(default_factory=lambda: datetime.now(UTC))
    realtime: bool = False

    def sync(self) -> datetime:
        if self.realtime:
            self.now = datetime.now(UTC)
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


@dataclass(frozen=True)
class OrderRequest:
    logical_id: str
    account_id: str
    tournament_id: str
    instrument_id: str
    side: str
    quantity: Decimal
    price: Decimal
    max_fee: Decimal = ZERO
    synthetic: bool = True


@dataclass(frozen=True)
class Fill:
    fill_id: str
    order_id: str
    instrument_id: str
    side: str
    quantity: Decimal
    price: Decimal
    fee: Decimal
    filled_at: datetime | None = None


@dataclass(frozen=True)
class Observation:
    order_id: str
    logical_id: str
    instrument_id: str
    side: str
    quantity: Decimal
    limit_price: Decimal
    filled: Decimal
    state: str
    revision: int
    fills: tuple[Fill, ...] = ()


TERMINAL = {"FILLED", "CANCELED", "REJECTED", "EXPIRED", "CLOSED"}
REMOTE_STATES = TERMINAL | {"UNOBSERVED", "OPEN", "PARTIAL"}


def merge_state(old: str, old_revision: int, new: Observation) -> str:
    if new.state not in REMOTE_STATES or new.filled < ZERO or new.filled > new.quantity:
        raise SchemaMismatch("UNKNOWN_ORDER_STATE_OR_QUANTITY")
    if new.revision <= old_revision:
        return old
    if old in TERMINAL and new.state != old:
        raise SchemaMismatch("TERMINAL_CHANGE_REQUIRES_CORRECTION")
    return new.state


def check_grid(price: Decimal, quantity: Decimal, tick: Decimal, step: Decimal) -> None:
    for value in (price, quantity, tick, step):
        decimal_value(value)
    if not 0 < price < 1 or quantity <= 0 or tick <= 0 or step <= 0:
        raise SchemaMismatch("INVALID_PRECISION")
    if price % tick or quantity % step:
        raise SchemaMismatch("INVALID_PRECISION")
