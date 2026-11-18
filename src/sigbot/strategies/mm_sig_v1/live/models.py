from decimal import Decimal
from typing import Literal

from pydantic import Field, model_validator

from sigbot.domain import Money, StrictModel
from sigbot.mm_risk_policy import MmRiskPolicy


class Settings(StrictModel):
    strategy_id: Literal["mm_sig"] = "mm_sig"
    strategy_version: Literal["1.1", "1.2", "1.3"] = "1.1"
    data_mode: Literal["REAL_ONLY"] = "REAL_ONLY"
    quote_quantity: Money | None = Field(default="10", gt=0, validate_default=True)
    max_inventory: Money | None = Field(default="20", gt=0, validate_default=True)
    max_new_exposure: Money | None = Field(default="30", gt=0, validate_default=True)
    max_event_cost: Money | None = Field(default="10", gt=0, validate_default=True)
    max_events: int | None = Field(default=3, ge=1)
    target_net_spread: Money | None = Field(default="0.02", ge=0, validate_default=True)
    exit_buffer: Money = Field(default="0.005", ge=0, validate_default=True)
    inventory_skew_ticks: int = Field(default=1, ge=0, le=5)
    book_ttl_seconds: int = Field(default=5, ge=1, le=5)
    account_refresh_seconds: int = Field(default=30, ge=30, le=60)
    account_ttl_seconds: int = Field(default=45, ge=30, le=45)
    catalog_refresh_seconds: int = Field(default=600, ge=300)
    activity_refresh_seconds: int = Field(default=60, ge=60)
    resting_order_seconds: int | None = Field(default=60, ge=30)
    quote_capital_fraction: Money | None = Field(default="0.001", gt=0, le=1, validate_default=True)
    event_capital_fraction: Money | None = Field(default="0.03", gt=0, le=1, validate_default=True)
    bootstrap_inventory: bool = False
    fee_file: str | None = None
    approval_file: str | None = None
    application_config: str | None = None
    execution_route: Literal["LOCAL", "SHARED", "OBSERVE"] = "LOCAL"
    shared_executor_config: str | None = None
    initial_capital_limit: Money | None = Field(default=None, gt=0)
    risk_policy: MmRiskPolicy | None = None
    bootstrap_inventory_mode: Literal["PASSIVE_ONLY"] = "PASSIVE_ONLY"
    bbo_refresh_seconds: int = Field(default=15, ge=15)
    tracked_book_refresh_seconds: int = Field(default=60, ge=60)
    selection_refresh_seconds: int = Field(default=300, ge=300)
    rotation_improvement_fraction: Money = Field(default="0.25", gt=0, validate_default=True)
    rotation_confirmation_rounds: int = Field(default=2, ge=2)
    exploration_event_cap: int = Field(default=1, ge=0, le=1)
    join_probe_enabled: bool = False
    join_probe_fraction: Money = Field(default="0.25", gt=0, le="0.25", validate_default=True)
    participation_model: Literal["OWN_FILL_AS", "CDJ_BOOK_IMBALANCE"] = "OWN_FILL_AS"
    pricing_model: Literal["LEGACY_132", "GUEANT_FINITE_GRID"] = "LEGACY_132"
    monitor_all_markets: bool = False
    shared_sig_source: str | None = None
    reviewed_nodes_file: str | None = None
    calibration_sampling_enabled: bool = False
    complementary_quotes_enabled: bool = False
    calibration_quote_fraction: Money | None = Field(
        default="0.1", gt=0, le=1, validate_default=True
    )
    calibration_exposure_fraction: Money | None = Field(
        default="0.001", gt=0, le=1, validate_default=True
    )
    monitor_interval_seconds: int = Field(default=5, ge=1, le=5)
    monitor_rest_audit_seconds: int = Field(default=1800, ge=300)
    model_risk_aversion: Money | None = Field(default=None, ge=0)
    model_depth_stress_fraction: Money = Field(default="0.5", gt=0, le=1, validate_default=True)
    model_comparisons: bool = False
    standard_calibration_execution: Literal["NONE", "EXISTING_APPROVED_POLICY"] = "NONE"
    signal_horizons_seconds: list[int] = Field(
        default_factory=lambda: [5, 30], min_length=2, max_length=2
    )
    signal_min_train_blocks: int = Field(default=20, ge=20)
    signal_min_validation_blocks: int = Field(default=10, ge=10)
    signal_uncertainty_ticks: Money = Field(default="0.1", gt=0, validate_default=True)
    per_side_margin_ticks: Money = Field(default="0.5", ge=0, validate_default=True)
    adverse_prior_ticks: Money = Field(default="0.5", ge=0, validate_default=True)
    quality_min_fills: int = Field(default=20, ge=2)
    quality_min_quote_seconds: int = Field(default=300, ge=60)
    own_fill_rate_fraction: Money = Field(default="0.5", gt=0, le=1, validate_default=True)
    target_turnover_minutes: int = Field(default=30, ge=1)
    capacity_unwind_minutes: int = Field(default=120, ge=1)
    exit_depth_band_ticks: int = Field(default=2, ge=1)
    exit_depth_fraction: Money = Field(default="0.10", gt=0, le=1, validate_default=True)
    exit_max_price_impact_fraction: Money = Field(default="0.01", gt=0, le=1, validate_default=True)
    exit_total_slippage_fraction: Money = Field(default="0.01", gt=0, le=1, validate_default=True)
    soft_oldest_lot_minutes: int = Field(default=60, ge=1)
    hard_oldest_lot_minutes: int = Field(default=120, ge=1)
    normal_reprice_distance_ticks: int = Field(default=2, ge=1)
    normal_resize_fraction: Money = Field(default="0.25", gt=0, le=1, validate_default=True)
    keep_remaining_fraction: Money = Field(default="0.5", gt=0, le=1, validate_default=True)
    normal_reprice_min_seconds: int = Field(default=60, ge=60)
    server_ttl_seconds: int | None = Field(default=None, ge=30, le=300)
    jump_window_seconds: int = Field(default=5, ge=1)
    jump_ticks: int = Field(default=2, ge=1)
    jump_cooldown_seconds: int = Field(default=60, ge=60)
    no_new_inventory_hours_before_close: int = Field(default=24, ge=0)
    exit_only_hours_before_close: int = Field(default=12, ge=0)
    priority_exit_hours_before_close: int = Field(default=2, ge=0)
    inventory_exit_min_price: Money | None = Field(default=None, gt=0, lt=1)
    global_cost_fraction: Money | None = Field(default=None, gt=0, le=1)
    state_cost_fraction: Money | None = Field(default=None, gt=0, le=1)

    @model_validator(mode="after")
    def legacy_scope(self):
        if self.strategy_version != "1.3" and (
            self.max_events is None
            or self.quote_capital_fraction is None
            or self.event_capital_fraction is None
        ):
            raise ValueError("legacy strategy requires its explicit numeric policy")
        if self.pricing_model != "LEGACY_132" and self.strategy_version != "1.3":
            raise ValueError("standard model requires MM shared version 1.3")
        if not 0 < self.signal_horizons_seconds[0] < self.signal_horizons_seconds[1] <= 300:
            raise ValueError("signal horizons must be increasing and no longer than 300s")
        if self.soft_oldest_lot_minutes > self.hard_oldest_lot_minutes:
            raise ValueError("soft lot age exceeds hard lot age")
        if not (
            self.no_new_inventory_hours_before_close
            >= self.exit_only_hours_before_close
            >= self.priority_exit_hours_before_close
        ):
            raise ValueError("invalid close risk schedule")
        if self.strategy_version == "1.3":
            if self.bootstrap_inventory or self.execution_route == "LOCAL":
                raise ValueError(
                    "MM 1.3 requires passive inventory and shared execution or observation"
                )
            if self.quote_quantity is not None:
                raise ValueError("MM 1.3 base quantity is derived from its capital budget")
            if self.initial_capital_limit is None:
                raise ValueError("MM 1.3 requires verified initial capital")
        if self.strategy_version == "1.1":
            if (
                any(
                    v is None or v > maximum
                    for v, maximum in (
                        (self.quote_quantity, 10),
                        (self.max_inventory, 20),
                        (self.max_new_exposure, 30),
                        (self.max_event_cost, 10),
                        (self.resting_order_seconds, 120),
                    )
                )
                or self.target_net_spread is None
                or self.target_net_spread < Decimal(".02")
            ):
                raise ValueError("legacy MM 1.1 limits unchanged; use competitive version 1.2")
        return self


def policy_capital(settings, actual):
    return (
        actual
        if settings.initial_capital_limit is None
        else min(settings.initial_capital_limit, actual)
    )


def quote_cash_budget(settings, capital, *, calibration=False):
    budget = (
        capital * settings.quote_capital_fraction
        if settings.quote_capital_fraction is not None
        else capital
    )
    if calibration and settings.calibration_quote_fraction is not None:
        budget *= settings.calibration_quote_fraction
    return budget


def event_cash_budget(settings, capital):
    budget = (
        capital * settings.event_capital_fraction
        if settings.event_capital_fraction is not None
        else capital
    )
    return min(budget, settings.max_event_cost) if settings.max_event_cost is not None else budget


def load_settings(path="configs/strategies/mm_sig_v1.real.json"):
    from pathlib import Path

    return Settings.model_validate_json(Path(path).read_text())
