"""Versioned MM risk preferences; candidates never authorize live execution."""

from datetime import datetime
from typing import Literal
from zoneinfo import ZoneInfo

from pydantic import Field, model_validator

from sigbot.domain import Money, StrictModel, content_hash


class MmRiskPolicy(StrictModel):
    version: str
    live_authorized: bool = False
    principal: Money = Field(default="50000", gt=0, validate_default=True)
    parent_cash: Money | None = Field(default=None, gt=0)
    parent_equity_fraction: Money | None = Field(default=None, gt=0, le=1)
    event_gross_cost: Money | None = Field(default=None, gt=0)
    mm_gross_cost: Money | None = Field(default=None, gt=0)
    episode_planned_loss: Money | None = Field(default=None, gt=0)
    episode_stress_loss: Money | None = Field(default=None, gt=0)
    event_stress_loss: Money | None = Field(default=None, gt=0)
    mm_stress_loss: Money | None = Field(default=None, gt=0)
    event_economic_loss: Money | None = Field(default=None, gt=0)
    episode_full_loss: Money | None = Field(default=None, gt=0)
    event_full_loss: Money | None = Field(default=None, gt=0)
    mm_full_loss: Money | None = Field(default=None, gt=0)
    daily_loss: Money | None = Field(default=None, gt=0)
    drawdown: Money | None = Field(default=None, gt=0)
    stress_depth_fraction: Money | None = Field(default=None, gt=0, le=1)
    stress_price_ticks: int | None = Field(default=None, ge=0, strict=True)
    stress_version: str | None = None
    stress_mode: Literal["DEPTH_SCENARIO", "FULL_PAID_COST_FALLBACK"] = "DEPTH_SCENARIO"
    fee_version: str | None = None
    target_profit_ticks: int | None = Field(default=None, ge=0, strict=True)
    entry_wait_seconds: int | None = Field(default=None, gt=0, strict=True)
    holding_seconds: int = Field(default=7200, gt=0, strict=True)
    exit_and_reconcile_seconds: int | None = Field(default=None, gt=0, strict=True)
    no_new_seconds: int | None = Field(default=None, ge=0, strict=True)
    reduce_only_seconds: int | None = Field(default=None, ge=0, strict=True)
    urgent_seconds: int | None = Field(default=None, ge=0, strict=True)
    reporting_timezone: str = "Asia/Shanghai"
    migration_hwm: Money | None = Field(default=None, gt=0)
    migration_day_equity: Money | None = Field(default=None, ge=0)
    migration_day: str | None = None
    migration_basis_approved: bool = False
    migration_day_method: Literal[
        "VERIFIED_DAY_OPEN", "DEPLOYMENT_NAV_WITH_CARRIED_REALIZED_LOSS"
    ] = "VERIFIED_DAY_OPEN"
    migration_carried_realized_loss: Money | None = Field(default=None, ge=0)
    new_orders_until: datetime | None = None

    @model_validator(mode="after")
    def ordered(self):
        ZoneInfo(self.reporting_timezone)
        if self.new_orders_until is not None and self.new_orders_until.tzinfo is None:
            raise ValueError("order deadline must include timezone")
        if all(
            v is not None
            for v in [self.no_new_seconds, self.reduce_only_seconds, self.urgent_seconds]
        ):
            if not self.no_new_seconds >= self.reduce_only_seconds >= self.urgent_seconds:
                raise ValueError("cutoff stages must be ordered")
        return self

    @property
    def digest(self):
        return content_hash(self.model_dump(mode="json"))

    def missing(self):
        return [
            name
            for name, value in self.model_dump().items()
            if value is None
            and not (
                self.stress_mode == "FULL_PAID_COST_FALLBACK"
                and name in {"stress_depth_fraction", "stress_price_ticks"}
            )
            and name
            not in {
                "migration_day_equity",
                "migration_day",
                "migration_carried_realized_loss",
                "new_orders_until",
            }
        ]
