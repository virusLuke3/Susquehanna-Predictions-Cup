from __future__ import annotations

from typing import Literal

from pydantic import Field, model_validator

from sigbot.domain import AwareTime, Clock, Money, StrictModel
from sigbot.errors import AuthorizationRequired


class Approval(StrictModel):
    """Local human-maintained, bounded approval checked by the unified executor."""

    approval_id: str = Field(min_length=1)
    operator: str = Field(min_length=1)
    operator_approved: Literal[True]
    accept_empirical_fee_model: bool = False
    accept_join_probe: bool = False
    accept_mm_calibration: bool = False
    mm_calibration_until: AwareTime | None = None
    issued_at: AwareTime
    new_orders_until: AwareTime
    cleanup_until: AwareTime
    account_id: str
    tournament_id: str
    instrument_allowlist: list[str] = Field(min_length=1)
    allowed_actions: list[Literal["SUBMIT", "CANCEL"]] = Field(min_length=1)
    max_order_count: int | None = Field(gt=0, strict=True)
    max_total_quantity: Money | None = Field(gt=0)
    max_total_notional: Money | None = Field(gt=0)
    per_order_limit: Money | None = Field(gt=0)
    price_min: Money = Field(gt=0, lt=1)
    price_max: Money = Field(gt=0, lt=1)
    code_hash: str = Field(pattern="^[0-9a-f]{64}$")
    config_hash: str = Field(pattern="^[0-9a-f]{64}$")
    api_contract_hash: str = Field(pattern="^[0-9a-f]{64}$")
    rules_hashes: dict[str, str]
    mapping_hashes: dict[str, str]
    cancel_scope: Literal["BOT_TRACKED_ONLY", "WHOLE_ACCOUNT"]
    expected_mode: Literal["LIVE_CANARY", "LIVE_LIMITED"]

    @model_validator(mode="after")
    def valid_window(self) -> Approval:
        if self.accept_mm_calibration and (
            self.mm_calibration_until is None
            or not self.issued_at < self.mm_calibration_until <= self.new_orders_until
        ):
            raise ValueError("MM calibration requires an explicit bounded approval window")
        if self.expected_mode != "LIVE_LIMITED" and any(
            v is None
            for v in (
                self.max_order_count,
                self.max_total_quantity,
                self.max_total_notional,
                self.per_order_limit,
            )
        ):
            raise ValueError("continuous budgets require LIVE_LIMITED authorization")
        if (
            not self.issued_at < self.new_orders_until <= self.cleanup_until
            or self.price_min > self.price_max
        ):
            raise ValueError("invalid approval window")
        return self

    def check(
        self,
        clock: Clock,
        bindings: dict,
        action: str,
        instrument: str,
        quantity,
        price,
        used_count=0,
        used_quantity=0,
        used_notional=0,
        bot_tracked=True,
    ) -> None:
        if clock.now < self.issued_at or clock.now >= (
            self.new_orders_until if action == "SUBMIT" else self.cleanup_until
        ):
            raise AuthorizationRequired("APPROVAL_EXPIRED")
        for key in (
            "account_id",
            "tournament_id",
            "code_hash",
            "config_hash",
            "api_contract_hash",
            "expected_mode",
        ):
            if bindings.get(key) != getattr(self, key):
                raise AuthorizationRequired("APPROVAL_BINDING_CHANGED")
        if action not in self.allowed_actions or instrument not in self.instrument_allowlist:
            raise AuthorizationRequired("APPROVAL_SCOPE_MISMATCH")
        # Existing instrument admission is bound above by account/tournament,
        # verified adapter/code, scope and time. Document versions are observed
        # by the callers; AGENTS 18 forbids revocation on those hashes alone.
        if action == "CANCEL":
            if not bot_tracked and self.cancel_scope != "WHOLE_ACCOUNT":
                raise AuthorizationRequired("CANCEL_SCOPE_MISMATCH")
            return
        notional = quantity * price
        if (
            quantity <= 0
            or not self.price_min <= price <= self.price_max
            or self.per_order_limit is not None
            and notional > self.per_order_limit
            or self.budget_exceeded(
                used_count + 1, used_quantity + quantity, used_notional + notional
            )
        ):
            raise AuthorizationRequired("APPROVAL_BUDGET_EXCEEDED")

    def budget_exceeded(self, count, quantity, notional, *, at_limit=False):
        return any(
            limit is not None and (used >= limit if at_limit else used > limit)
            for used, limit in (
                (count, self.max_order_count),
                (quantity, self.max_total_quantity),
                (notional, self.max_total_notional),
            )
        )
