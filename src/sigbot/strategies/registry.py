"""Explicit strategy identities; no dynamic imports from user-provided paths."""

from dataclasses import dataclass
from pathlib import Path

from sigbot.errors import RiskRejected


@dataclass(frozen=True)
class StrategySpec:
    strategy_id: str
    version: str
    config: str
    report_directory: str

    def check_output(self, path: str) -> Path:
        target = Path(path).resolve()
        if target.suffix != ".json" or not target.is_relative_to(
            Path(self.report_directory).resolve()
        ):
            raise ValueError("strategy output must stay in its own report directory")
        target.parent.mkdir(parents=True, exist_ok=True)
        return target


ECR = StrategySpec(
    "ecr_sig",
    "1.0",
    "configs/strategies/ecr_sig_v1.json",
    "reports/strategies/ecr_sig_v1",
)

PRC = StrategySpec(
    "prc_sig",
    "1.0",
    "configs/strategies/prc_sig_v1.json",
    "reports/strategies/prc_sig_v1",
)

ECR_REFERENCE = StrategySpec(
    "ecr_sig",
    "1.1",
    "configs/strategies/ecr_sig_v1.reference.json",
    "reports/strategies/ecr_sig_v1",
)

MM = StrategySpec(
    "mm_sig",
    "1.0",
    "configs/strategies/mm_sig_v1.json",
    "reports/strategies/mm_sig_v1",
)

MM_REAL = StrategySpec(
    "mm_sig",
    "1.1",
    "configs/strategies/mm_sig_v1.real.json",
    "reports/strategies/mm_sig_v1/production",
)

MM_COMPETITIVE = StrategySpec(
    "mm_sig",
    "1.2",
    "configs/strategies/mm_sig_v1.competitive.json",
    "reports/strategies/mm_sig_v1/production",
)

STRATEGIES = (ECR, ECR_REFERENCE, PRC, MM, MM_REAL, MM_COMPETITIVE)


def resolve(strategy_id: str, version: str) -> StrategySpec:
    for strategy in STRATEGIES:
        if (strategy_id, version) == (strategy.strategy_id, strategy.version):
            return strategy
    raise RiskRejected("STRATEGY_NOT_REGISTERED")
