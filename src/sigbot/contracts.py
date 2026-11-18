import json
from pathlib import Path

from sigbot.errors import ContractNotVerified

REQUIRED_FIELDS = (
    "http_method",
    "path",
    "request_schema_ref",
    "response_schema_ref",
    "authentication_ref",
    "business_permission",
    "rate_budget_class",
    "rate_weight",
    "id_scope",
    "decimal_units",
    "error_semantics",
    "evidence",
)


class Contract:
    def __init__(self, path: str):
        self.path = Path(path)
        self.data = json.loads(self.path.read_text())

    def blockers(self, name: str) -> list[str]:
        c = self.data.get("capabilities", {}).get(name)
        if not c:
            return ["CAPABILITY_MISSING"]
        missing = [k for k in REQUIRED_FIELDS if not c.get(k)]
        if c.get("status") != "VERIFIED":
            missing.append("OFFICIAL_CONTRACT_UNVERIFIED")
        if not c.get("call_enabled"):
            missing.append("CALL_DISABLED")
        if not self.data.get("production_contract_verified"):
            missing.append("PRODUCTION_CONTRACT_UNVERIFIED")
        for key in ("header_name", "value_format", "scope"):
            if not self.data.get("auth", {}).get(key):
                missing.append("auth." + key)
        # Evidence paths must resolve to immutable local bytes, not only a claimed status flag.
        if c.get("status") == "VERIFIED":
            import hashlib

            for ev in c.get("evidence", []):
                path = Path(ev.get("evidence_path", ""))
                if (
                    ev.get("source") != "OFFICIAL"
                    or not path.is_file()
                    or hashlib.sha256(path.read_bytes()).hexdigest() != ev.get("sha256")
                    or not ev.get("source_url", "").startswith("https://sig.thesuper.market/")
                    or not ev.get("observed_at")
                    or not ev.get("review_policy")
                ):
                    missing.append("INVALID_OFFICIAL_EVIDENCE")
        return sorted(set(missing))

    def require(self, name: str) -> dict:
        if self.blockers(name):
            raise ContractNotVerified("CONTRACT_UNVERIFIED:" + name)
        return self.data["capabilities"][name]

    def require_documented_read(self, name: str) -> dict:
        """Evidence-backed GET for an explicitly authorized read-only session.

        Observing a read does not verify the production economic contract or enable writes.
        """
        import hashlib

        c = self.data.get("capabilities", {}).get(name)
        if (
            not c
            or c.get("status") not in {"DOCUMENTED", "VERIFIED"}
            or c.get("http_method") != "GET"
            or c.get("business_permission") not in {"READ_ACCOUNT", "READ_MARKET"}
            or c.get("rate_budget_class") != "READ"
            or c.get("rate_weight") != 1
            or any(not c.get(k) for k in REQUIRED_FIELDS)
            or c.get("path") != "/api/v1" + c.get("openapi_path", "")
        ):
            raise ContractNotVerified("DOCUMENTED_READ_UNAVAILABLE:" + name)
        for ev in c["evidence"]:
            path = Path(ev.get("evidence_path", ""))
            if (
                ev.get("source") != "OFFICIAL"
                or not path.is_file()
                or hashlib.sha256(path.read_bytes()).hexdigest() != ev.get("sha256")
                or ev.get("source_url") != "https://sig.thesuper.market/api/v1/docs"
                or not ev.get("observed_at")
                or not ev.get("review_policy")
            ):
                raise ContractNotVerified("INVALID_OFFICIAL_EVIDENCE")
        return c

    def require_documented_probe(self, name: str) -> dict:
        """Documented single submit/cancel, only callable by the fixed one-share executor.

        Does not change normal production Contract.require or approval templates.
        """
        import hashlib

        expected = {
            "submit_limit": ("POST", "/api/v1/orders"),
            "cancel_one": ("DELETE", "/api/v1/orders/{id}"),
        }
        c = self.data.get("capabilities", {}).get(name)
        if (
            name not in expected
            or not c
            or (c.get("http_method"), c.get("path")) != expected[name]
            or c.get("status") != "DOCUMENTED"
            or any(not c.get(k) for k in REQUIRED_FIELDS)
        ):
            raise ContractNotVerified("DOCUMENTED_PROBE_UNAVAILABLE")
        for ev in c["evidence"]:
            p = Path(ev.get("evidence_path", ""))
            if (
                ev.get("source") != "OFFICIAL"
                or not p.is_file()
                or hashlib.sha256(p.read_bytes()).hexdigest() != ev.get("sha256")
                or ev.get("source_url") != "https://sig.thesuper.market/api/v1/docs"
            ):
                raise ContractNotVerified("INVALID_OFFICIAL_EVIDENCE")
        return c

    def require_documented_execution(self, name: str) -> dict:
        """Verify the used official operation, without enabling unrelated capabilities.

        Permission is enforced separately by the persisted OMS and human approval.
        Unknown batch, collateral and fee semantics do not turn into verified flags.
        """
        if name not in {"submit_limit", "cancel_one"}:
            raise ContractNotVerified("EXECUTION_CAPABILITY_UNAVAILABLE")
        return self.require_documented_probe(name)

    def report(self) -> dict:
        return {name: self.blockers(name) for name in self.data["capabilities"]}
