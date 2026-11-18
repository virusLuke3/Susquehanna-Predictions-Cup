"""Complete official account and selected-market evidence; never synthetic data."""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

from sigbot.accountcheck import checkpoint
from sigbot.domain import content_hash
from sigbot.errors import AuthorizationRequired, ContractNotVerified, SchemaMismatch
from sigbot.history import HistoryReads
from sigbot.readcheck import load_key
from sigbot.strategies.mm_sig_v1.data import current_reference, reviewed_schema
from sigbot.transport import OFFICIAL_ORIGIN

ROOT = Path("runtime/readonly/strategies/mm_sig_v1")
REPORTS = Path("reports/strategies/mm_sig_v1/production")


def save(path: Path, value, *, private=False):
    raw = (json.dumps(value, default=str, ensure_ascii=False, indent=2) + "\n").encode()
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700 if private else 0o755)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_bytes(raw)
    if private:
        tmp.chmod(0o600)
    tmp.replace(path)


class RealReads(HistoryReads):
    def __init__(
        self,
        *,
        env_file=".env",
        slug="midterm-elections",
        seconds=900,
        max_requests=500,
        verify_reference=True,
    ):
        contract, schema = reviewed_schema()
        if verify_reference:
            current_reference(REPORTS / "reference", schema)
        super().__init__(
            load_key(env_file),
            tournament_slug=slug,
            root=Path("runtime/readonly"),
            seconds=seconds,
            max_requests=max_requests,
            contract_file="contracts/api-contract.working.json",
        )
        self.private = ROOT / "sessions" / self.run_id
        self.reader.schema = schema
        for name in contract["read_capabilities"]:
            cap = self.reader.contract.data["capabilities"].get(name)
            if (
                cap is None
                or cap.get("http_method") != "GET"
                or cap.get("business_permission") not in {"READ_MARKET", "READ_ACCOUNT"}
            ):
                raise ContractNotVerified("MM_READ_CAPABILITY_UNAVAILABLE:" + name)
            if cap["openapi_path"] not in schema.spec["paths"]:
                raise ContractNotVerified("MM_READ_SCHEMA_MISSING:" + name)
            cap["evidence"] = [
                deepcopy(
                    {
                        "source": "OFFICIAL",
                        "evidence_path": schema.path,
                        "sha256": hashlib.sha256(Path(schema.path).read_bytes()).hexdigest(),
                        "source_url": OFFICIAL_ORIGIN + "/api/v1/docs",
                        "observed_at": contract["reviewed_on"],
                        "review_policy": contract["review_scope"],
                    }
                )
            ]
            self.reader.contract.require_documented_read(name)

    def all_pages(self, name, **kwargs):
        self.checks[name] = {}
        return self.pages(name, **kwargs)[0]

    def bootstrap(self):
        previous_account = self.reader.config.expected_account_id
        tid = self.reader.config.expected_tournament_id
        identity = self.call(
            "account_identity", **({"query": {"tournamentId": tid}} if tid else {})
        )
        aid = identity["id"]
        if not aid or aid.startswith("SIM-") or previous_account not in {None, aid}:
            raise AuthorizationRequired("REAL_ACCOUNT_REQUIRED")
        self.reader.config.expected_account_id = aid
        cups = self.all_pages("tournament_list", query={"status": "any", "limit": 100, "offset": 0})
        matches = [c for c in cups if c["slug"] == self.slug]
        if len(matches) != 1 or matches[0]["isPendingEnrolment"]:
            raise AuthorizationRequired("MM_TOURNAMENT_SCOPE_NOT_VERIFIED")
        tid = matches[0]["id"]
        self.reader.config.expected_tournament_id = tid
        self.reader.config.expected_tournament_slug = self.slug
        return aid, tid

    def checked_account(self):
        result, data = checkpoint(self)
        save(
            self.private / "account-checkpoint.json", {"result": result, "data": data}, private=True
        )
        return result, data

    def market_nodes(self, mid):
        tid = self.reader.config.expected_tournament_id
        raw = self.call("market_nodes", path_params={"id": mid}, query={"tournamentId": tid})
        if (
            raw["market_id"] != mid
            or not raw["contexts"]
            or any(
                c["type"] != "tournament" or c["tournament"]["id"] != tid for c in raw["contexts"]
            )
        ):
            raise SchemaMismatch("MM_RULES_TOURNAMENT_SCOPE_MISMATCH")
        return raw

    def close(self):
        self.reader.close()
        self.key = ""


def audit_real_account():
    reads = RealReads(max_requests=80, seconds=180)
    try:
        aid, tid = reads.bootstrap()
        result, data = reads.checked_account()
        details = []
        for pos in data["positions"]["positions"]:
            nodes = reads.market_nodes(pos["marketId"])
            details.append(
                {
                    "market_id": pos["marketId"],
                    "exchange_id": pos["exchangeId"],
                    "node_hash": content_hash(nodes),
                    "nodes": nodes,
                    "position": pos,
                }
            )
        save(reads.private / "position-rules.json", details, private=True)
        public = {k: v for k, v in result.items() if k not in {"journal"}}
        public.update(
            layer="REAL_READ",
            account_id_hash=content_hash(aid),
            tournament_id=tid,
            started_at=reads.report["started_at"],
            ended_at=datetime.now(timezone.utc).isoformat(),
            read_attempts=reads.total_attempts,
            private_evidence=str(reads.private),
            requests=reads.requests,
            real_orders=0,
            real_cancels=0,
            synthetic_data_used=False,
        )
        save(REPORTS / "ACCOUNT_INITIAL.json", public)
        return public
    finally:
        reads.close()


if __name__ == "__main__":
    print(json.dumps(audit_real_account(), default=str, ensure_ascii=False, indent=2))
