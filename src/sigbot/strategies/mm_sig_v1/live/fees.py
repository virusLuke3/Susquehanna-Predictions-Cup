"""Empirical fee estimate, explicitly separate from an official future fee schedule."""

import hashlib
import json
from pathlib import Path
from typing import Literal

from pydantic import Field

from sigbot.domain import Money, StrictModel
from sigbot.errors import ContractNotVerified
from sigbot.live import ExecutionFee


class EmpiricalFee(ExecutionFee):
    kind: Literal["EMPIRICAL_ACTUAL_TRADE_CALIBRATION"]
    per_share: Money = Field(ge=0, le=0)
    verified: Literal[False] = False

    def check(self, mock=False):
        path = Path(self.evidence_path)
        if (
            mock
            or not path.is_file()
            or hashlib.sha256(path.read_bytes()).hexdigest() != self.sha256
        ):
            raise ContractNotVerified("EMPIRICAL_FEE_ACTUAL_EVIDENCE_REQUIRED")
        data = json.loads(path.read_text())
        if (
            data.get("layer") != "LIVE_CANARY"
            or data.get("synthetic_data_used") is not False
            or data.get("status") != "PASS_EMPIRICAL_SCOPE"
            or data.get("state") != "CLEAN"
            or data.get("fee_event_count") != 0
            or not data.get("existing_positions_preserved")
            or data.get("retained_test_position")
        ):
            raise ContractNotVerified("EMPIRICAL_FEE_CALIBRATION_NOT_ACCEPTED")
        if any(
            data.get("legs", {}).get(side, {}).get("filled_quantity") != "1"
            for side in ("buy", "sell")
        ):
            raise ContractNotVerified("EMPIRICAL_FEE_BOTH_ACTUAL_SIDES_REQUIRED")


class OperatorFixedFee(StrictModel):
    """A user-selected cost parameter, with no claim about an official fee schedule."""

    kind: Literal["OPERATOR_FIXED_FEE"]
    per_share: Money = Field(ge=0, le=0)
    evidence_path: str
    sha256: str = Field(pattern="^[0-9a-f]{64}$")
    account_id_hash: str
    tournament_id: str
    strategy_id: Literal["ssr_sig", "grid_sig"]
    strategy_version: Literal["1.0", "2.0"]

    def check(self, mock=False):
        if self.strategy_version == "2.0" and self.strategy_id != "grid_sig":
            raise ContractNotVerified("FIXED_COST_UNREGISTERED_VERSION")
        path = Path(self.evidence_path)
        if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != self.sha256:
            raise ContractNotVerified("FIXED_COST_INSTRUCTION_BINDING_CHANGED")
        data = json.loads(path.read_text())
        if (
            any(
                data.get(k) != getattr(self, k)
                for k in ("account_id_hash", "tournament_id", "strategy_id", "strategy_version")
            )
            or data.get("extra_fee_per_share") != "0"
            or data.get("source") != "USER_INSTRUCTION"
        ):
            raise ContractNotVerified("FIXED_COST_INSTRUCTION_SCOPE_MISMATCH")


def load_fee(path):
    raw = Path(path).read_text()
    kind = json.loads(raw).get("kind")
    if kind == "EMPIRICAL_ACTUAL_TRADE_CALIBRATION":
        return EmpiricalFee.model_validate_json(raw)
    if kind == "OPERATOR_FIXED_FEE":
        return OperatorFixedFee.model_validate_json(raw)
    return ExecutionFee.model_validate_json(raw)
