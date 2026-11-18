from pathlib import Path

from pydantic import Field, model_validator

from sigbot.domain import Mode, Money, StrictModel
from sigbot.mm_risk_policy import MmRiskPolicy


class RiskLimits(StrictModel):
    max_order_notional: Money | None = Field(default=None, gt=0)
    max_total_exposure: Money | None = Field(default=None, gt=0)
    max_event_exposure: Money | None = Field(default=None, gt=0)
    max_instrument_quantity: Money | None = Field(default=None, gt=0)
    max_open_orders: int | None = Field(default=None, gt=0, strict=True)


class Config(StrictModel):
    mode: Mode = Mode.OFFLINE
    namespace: str = "offline"
    business_network_enabled: bool = False
    read_session_authorized: bool = False
    live_trading_enabled: bool = False
    parallel_strategy_execution: bool = False
    expected_account_id: str | None = None
    expected_tournament_id: str | None = None
    expected_tournament_slug: str | None = None
    allowed_instruments: list[str] = Field(default_factory=list)
    database_url: str = "sqlite+pysqlite:///runtime/offline/sigbot.db"
    contract_file: str = "contracts/api-contract.working.json"
    live_fee_file: str | None = None
    raw_output_dir: str = "runtime/offline/raw"
    risk: RiskLimits = Field(default_factory=RiskLimits)
    strategy_capital_allocations: dict[str, Money] = Field(default_factory=dict)
    mm_risk_policy: MmRiskPolicy | None = None
    read_budget: int = Field(default=80, gt=0, le=100, strict=True)
    write_budget: int = Field(default=24, gt=0, le=30, strict=True)
    new_order_budget: int = Field(default=16, gt=0, le=30, strict=True)
    book_ttl_seconds: int = Field(default=10, gt=0, strict=True)
    account_ttl_seconds: int = Field(default=45, gt=0, strict=True)
    drain_seconds: int = Field(default=300, ge=0, strict=True)

    @model_validator(mode="after")
    def gates(self) -> "Config":
        if any(not name or amount <= 0 for name, amount in self.strategy_capital_allocations.items()):
            raise ValueError("strategy capital allocations must be positive")
        if self.parallel_strategy_execution and self.mode != Mode.LIVE_LIMITED:
            raise ValueError("parallel strategy execution requires LIVE_LIMITED")
        if self.new_order_budget > self.write_budget:
            raise ValueError("new budget is a subset of writes")
        namespace = {
            Mode.OFFLINE: "offline",
            Mode.SHADOW: "shadow",
            Mode.READ_ONLY: "readonly",
            Mode.SETTLEMENT_ONLY: "readonly",
            Mode.LIVE_CANARY: "live",
            Mode.LIVE_LIMITED: "live",
        }[self.mode]
        if self.namespace != namespace or namespace not in Path(self.raw_output_dir).parts:
            raise ValueError("mode namespace/output mismatch")
        if self.mode == Mode.OFFLINE and (
            self.business_network_enabled or self.live_trading_enabled
        ):
            raise ValueError("OFFLINE cannot enable business network or live writes")
        if (
            self.mode in (Mode.READ_ONLY, Mode.SHADOW, Mode.SETTLEMENT_ONLY)
            and self.live_trading_enabled
        ):
            raise ValueError("read modes cannot enable live writes")
        if self.mode in (Mode.LIVE_CANARY, Mode.LIVE_LIMITED):
            if not all(
                [
                    self.live_trading_enabled,
                    self.business_network_enabled,
                    self.read_session_authorized,
                    self.expected_account_id,
                    self.expected_tournament_id,
                    self.allowed_instruments,
                ]
            ):
                raise ValueError("incomplete live configuration")
            if self.mode == Mode.LIVE_CANARY and any(
                v is None for v in self.risk.model_dump().values()
            ):
                raise ValueError("live risk limits required")
        return self

    @classmethod
    def load(cls, path: str | None) -> "Config":
        return cls.model_validate_json(Path(path).read_text()) if path else cls()
