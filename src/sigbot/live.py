"""Official SIG limit orders through the persistent, single-account OMS.

No automatic submission retries. A receipt is persisted before economic decoding;
reporting uses complete order-fill pages and a bracketed order read. The local
projection revision is explicitly local, not an invented exchange sequence.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import threading
import time
from dataclasses import asdict
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import TextIO

import httpx
from pydantic import Field
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from sigbot.accountcheck import checkpoint
from sigbot.adapters import AccountSnapshot
from sigbot.authorization import Approval
from sigbot.config import Config
from sigbot.contracts import Contract
from sigbot.domain import (
    TERMINAL,
    ZERO,
    Clock,
    Fill,
    Mode,
    Money,
    Observation,
    StrictModel,
    content_hash,
    utc_time,
)
from sigbot.errors import (
    AuthorizationRequired,
    Conflict,
    ContractNotVerified,
    HistoryReadError,
    OrderRejected,
    RateLimited,
    SchemaMismatch,
    SigbotError,
    StorageUnavailable,
    SubmissionNotSent,
    TransportUnknown,
)
from sigbot.official import DocumentedReads, OfficialSchema, documented_limit_payload, pointer
from sigbot.orderprobe import exact_body
from sigbot.readcheck import ReadBudget, private_write
from sigbot.storage import (
    Audit,
    FillRow,
    Instrument,
    IntentRow,
    LiveWire,
    Market,
    Order,
    Position,
    PositionLot,
)
from sigbot.storage_policy import save_read_evidence
from sigbot.transport import OFFICIAL_ORIGIN


def code_hash(root: Path | None = None) -> str:
    root = root or Path(__file__).resolve().parent
    return content_hash(
        [
            ("src/sigbot/" + str(p.relative_to(root)), hashlib.sha256(p.read_bytes()).hexdigest())
            for p in sorted(root.rglob("*.py"))
        ]
    )


def bindings(config: Config) -> dict:
    return {
        "account_id": config.expected_account_id,
        "tournament_id": config.expected_tournament_id,
        "code_hash": code_hash(),
        "config_hash": content_hash(config.model_dump(mode="json")),
        "api_contract_hash": hashlib.sha256(Path(config.contract_file).read_bytes()).hexdigest(),
        "expected_mode": config.mode.value,
    }


class ExecutionFee(StrictModel):
    """An explicit fixed per-share economic contract; absence never means zero.

    Evidence is operator-reviewed official material, bound by hash. The mock test
    fixture is accepted only with an in-process MockTransport. Variable fee models
    need a separate decoder, rather than pretending an upper bound is actual fee.
    """

    per_share: Money = Field(ge=0)
    source_url: str
    evidence_path: str
    sha256: str = Field(pattern="^[0-9a-f]{64}$")
    verified: bool = False

    def check(self, mock: bool = False) -> None:
        p = Path(self.evidence_path)
        if (
            not self.verified
            or not p.is_file()
            or hashlib.sha256(p.read_bytes()).hexdigest() != self.sha256
        ):
            raise ContractNotVerified("EXECUTION_FEE_EVIDENCE_REQUIRED")
        if not self.source_url.startswith("https://sig.thesuper.market/") and not (
            mock and self.source_url == "MOCK"
        ):
            raise ContractNotVerified("OFFICIAL_EXECUTION_FEE_EVIDENCE_REQUIRED")


class SigExecutionExchange:
    synthetic = False

    def __init__(
        self,
        store,
        config: Config | None = None,
        clock: Clock | None = None,
        key: str | None = None,
        approval: Approval | None = None,
        *,
        transport=None,
        budget_path=Path("runtime/readonly/shared-read-budget.json"),
    ):
        if config is None or clock is None or key is None:
            raise ContractNotVerified("LIVE_EXECUTION_CONTEXT_REQUIRED")
        if store.namespace != "live" or store.engine.dialect.name != "postgresql":
            raise AuthorizationRequired("LIVE_POSTGRES_NAMESPACE_REQUIRED")
        if (
            config.mode not in {Mode.LIVE_CANARY, Mode.LIVE_LIMITED}
            or not config.live_trading_enabled
        ):
            raise AuthorizationRequired("LIVE_CONFIGURATION_REQUIRED")
        self.store, self.config, self.clock, self.key, self.approval = (
            store,
            config,
            clock,
            key,
            approval,
        )
        assert config.expected_account_id is not None and config.expected_tournament_id is not None
        self.account_id, self.tournament_id = (
            config.expected_account_id,
            config.expected_tournament_id,
        )
        self.mock = isinstance(transport, httpx.MockTransport)
        if not self.mock:
            self.clock.realtime = True
        self.contract = Contract(config.contract_file)
        self.schema = OfficialSchema(
            self.contract.data.get("schema_path", "evidence/api/openapi-2026-10-03.json")
        )
        self.budget = ReadBudget(budget_path, 80, 600)
        self.read_budget_lock = threading.Lock()
        self.read_receipts: list[dict] = []
        self.reader = DocumentedReads(
            config,
            self.contract,
            self.charge_read,
            self.pause,
            transport=transport,
            observe=self.archive_read,
            documented_read_session=True,
        )
        self.writer = httpx.Client(
            transport=transport,
            trust_env=False,
            follow_redirects=False,
            timeout=httpx.Timeout(15, connect=5),
        )
        self.fee = None
        self.empirical_fee_accepted_by_operator = bool(
            approval and approval.accept_empirical_fee_model
        )
        if config.live_fee_file:
            from sigbot.strategies.mm_sig_v1.live.fees import load_fee

            self.fee = load_fee(config.live_fee_file)
        self.call_count = {"POST": 0, "DELETE": 0}
        self.contract_review_error: str | None = None
        self.execution_until: datetime | None = None
        self.write_lock: TextIO | None = None
        self.lock_path = (
            budget_path.parent / "executor.lock"
            if self.mock
            else Path("runtime/live/order-probes/executor.lock")
        )

    def acquire_write_lock(self):
        self.lock_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        handle = self.lock_path.open("a")
        try:
            kind = fcntl.LOCK_SH if self.config.parallel_strategy_execution else fcntl.LOCK_EX
            fcntl.flock(handle.fileno(), kind | fcntl.LOCK_NB)
        except BlockingIOError:
            handle.close()
            raise Conflict("LIVE_EXECUTOR_ALREADY_OWNED") from None
        self.write_lock = handle
        legacy = Path("runtime/live/order-probes/active.json")
        if not self.mock and legacy.exists():
            prior = json.loads(legacy.read_text())
            if prior.get("state") in {"UNKNOWN", "SUBMITTING", "CANCEL_PENDING", "ACKNOWLEDGED"}:
                self.release_write_lock()
                raise Conflict("LEGACY_PROBE_REQUEST_UNRESOLVED")

    def check_write_lock(self):
        if self.write_lock is None or self.write_lock.closed:
            raise StorageUnavailable("LIVE_EXECUTOR_NOT_OWNER")

    def release_write_lock(self):
        if self.write_lock is not None:
            self.write_lock.close()
            self.write_lock = None

    def pause(self, reason):
        from sigbot.operations import Controls

        Controls(self.store, self.clock).pause(self.account_id, reason)

    def archive_read(self, name, status, raw):
        digest = hashlib.sha256(raw).hexdigest()
        p = Path(self.config.raw_output_dir) / (name + "-" + digest + ".json")
        save_read_evidence(p, name, status, raw)
        self.read_receipts.append({"capability": name, "http_status": status, "sha256": digest})
        if not self.mock:
            from datetime import datetime, timezone

            self.clock.now = datetime.now(timezone.utc)

    def authorized(self) -> None:
        if self.approval is None:
            raise AuthorizationRequired("OPERATOR_APPROVAL_REQUIRED")
        if self.fee is None:
            raise ContractNotVerified("EXECUTION_FEE_EVIDENCE_REQUIRED")
        self.fee.check(self.mock)
        if getattr(self.fee, "kind", None) == "OPERATOR_FIXED_FEE" and (
            self.fee.account_id_hash != content_hash(self.config.expected_account_id)
            or self.fee.tournament_id != self.config.expected_tournament_id
        ):
            raise AuthorizationRequired("FIXED_COST_ACCOUNT_OR_STRATEGY_SCOPE_MISMATCH")
        if getattr(self.fee, "kind", None) == "EMPIRICAL_ACTUAL_TRADE_CALIBRATION" and not getattr(
            self, "empirical_fee_accepted_by_operator", False
        ):
            raise AuthorizationRequired("EMPIRICAL_FEE_MODEL_REQUIRES_EXPLICIT_OPERATOR_ACCEPTANCE")
        if not (self.config.business_network_enabled and self.config.read_session_authorized):
            raise AuthorizationRequired("LIVE_CONFIGURATION_REQUIRED")

    def approve(self, s, account, order, action):
        self.authorized()
        assert self.approval is not None
        row = s.get(IntentRow, order.intent_id)
        if row and row.payload.get("strategy_id") == "external_observed":
            raise AuthorizationRequired("EXTERNAL_OBSERVATION_HAS_NO_WRITE_AUTHORITY")
        fee = getattr(self, "fee", None)
        if getattr(fee, "kind", None) == "OPERATOR_FIXED_FEE" and (
            row is None
            or row.payload.get("strategy_id") != getattr(fee, "strategy_id", None)
            or row.payload.get("strategy_version") != getattr(fee, "strategy_version", None)
        ):
            raise AuthorizationRequired("FIXED_COST_STRATEGY_SCOPE_MISMATCH")
        if (
            action == "SUBMIT"
            and row is not None
            and row.payload.get("strategy_id") == "mm_sig"
            and json.loads(row.payload.get("note") or "{}")
            .get("policy", {})
            .get("calibration_sampling")
            and (
                not self.approval.accept_mm_calibration
                or self.approval.mm_calibration_until is None
                or self.clock.now >= self.approval.mm_calibration_until
            )
        ):
            raise AuthorizationRequired("MM_CALIBRATION_OPERATOR_APPROVAL_REQUIRED")
        inst = s.get(Instrument, order.instrument_id)
        if (
            inst is None
            or not inst.exchange_id
            or not inst.exchange_id.isdecimal()
            or inst.id.startswith("SIM-")
        ):
            raise SchemaMismatch("REAL_INSTRUMENT_MAPPING_REQUIRED")
        rows = list(
            s.scalars(select(LiveWire).where(LiveWire.approval_id == self.approval.approval_id))
        )
        digest = content_hash(self.approval.model_dump(mode="json"))
        if any(row.approval_digest != digest for row in rows):
            raise AuthorizationRequired("APPROVAL_ID_CONTENT_CHANGED")
        if action == "SUBMIT":
            rows = [row for row in rows if row.order_id != order.id]
        used_quantity = sum((Decimal(row.payload["quantity"]) for row in rows), ZERO)
        used_notional = sum(
            (Decimal(row.payload["quantity"]) * Decimal(row.payload["price"]) for row in rows), ZERO
        )
        bind = {
            **bindings(self.config),
            "rules_hash": inst.rules_hash,
            "mapping_hash": inst.mapping_hash,
        }
        self.approval.check(
            self.clock,
            bind,
            action,
            inst.id,
            order.quantity,
            order.price,
            used_count=len(rows),
            used_quantity=used_quantity,
            used_notional=used_notional,
        )
        if (account.id, account.tournament) != (
            self.config.expected_account_id,
            self.config.expected_tournament_id,
        ):
            raise AuthorizationRequired("LIVE_IDENTITY_MISMATCH")
        if inst.id not in self.config.allowed_instruments:
            raise AuthorizationRequired("LIVE_INSTRUMENT_NOT_ALLOWED")
        if action == "SUBMIT":
            assert self.fee is not None
            if (
                inst.fee_per_share != self.fee.per_share
                or order.fee_bound != self.fee.per_share * order.quantity
            ):
                raise ContractNotVerified("EXECUTION_FEE_BOUND_MISMATCH")
            from sigbot.accounting import binary_partner, complementary_mm

            for other in s.scalars(
                select(Instrument).where(
                    Instrument.exchange_id == inst.exchange_id,
                    Instrument.tournament == account.tournament,
                )
            ):
                pos = s.get(Position, (account.id, other.id))
                if other.outcome != inst.outcome and pos and pos.quantity:
                    if not complementary_mm(row) or order.side != "BUY":
                        raise SchemaMismatch("OPPOSITE_POSITION_NETTING_UNSUPPORTED")
                    if binary_partner(s, inst).id != other.id:
                        raise SchemaMismatch("BINARY_NETTING_MAPPING_UNVERIFIED")
                    for lot in s.scalars(
                        select(PositionLot).where(
                            PositionLot.account_id == account.id,
                            PositionLot.instrument_id == other.id,
                            PositionLot.quantity > ZERO,
                        )
                    ):
                        fill = s.get(FillRow, (account.id, lot.economic_id.removeprefix("fill:")))
                        origin = s.get(Order, fill.order_id) if fill else None
                        owner = s.get(IntentRow, origin.intent_id) if origin else None
                        if owner is None or owner.payload.get("strategy_id") != "mm_sig":
                            raise SchemaMismatch("NETTING_FOREIGN_INVENTORY_PROTECTED")
            if order.side == "SELL":
                pos = s.get(Position, (account.id, inst.id))
                if pos is None or pos.quantity < order.quantity:
                    raise SchemaMismatch("COMPLEMENT_SELL_UNSUPPORTED")
        return inst

    def limit_expiration(self, payload):
        if (payload.get("strategy_id"), payload.get("strategy_version")) in {
            ("grid_sig", "1.0"),
            ("grid_sig", "2.0"),
        }:
            from sigbot.strategies.grid_sig import Parameters

            if payload["strategy_version"] == "2.0":
                from sigbot.strategies.grid_risk import Parameters
            metadata = json.loads(payload["note"])
            grid_policy = Parameters.model_validate(metadata["parameters"])
            lifetime = grid_policy.order_seconds
            if payload["strategy_version"] == "2.0":
                from sigbot.strategies.grid_execution import order_seconds

                lifetime = order_seconds(metadata, lifetime)
            assert self.approval is not None
            deadline = min(
                utc_time(payload["created_at"]) + timedelta(seconds=lifetime),
                self.approval.new_orders_until,
            )
            if metadata.get("holding_deadline") and metadata.get("kind") == "ENTRY":
                deadline = min(deadline, utc_time(metadata["holding_deadline"]))
            if metadata.get("replacement_deadline") and metadata.get("kind") == "ENTRY":
                deadline = min(deadline, utc_time(metadata["replacement_deadline"]))
            return deadline.isoformat()
        if payload.get("strategy_id") != "mm_sig":
            return None
        if self.config.mm_risk_policy is not None and payload.get("action") == "INCREASE_TO_TARGET":
            assert self.approval is not None
            return min(utc_time(payload["created_at"])+timedelta(seconds=self.config.mm_risk_policy.entry_wait_seconds),
                       self.approval.new_orders_until).isoformat()
        if payload.get("strategy_version") == "1.1":
            return utc_time(payload["expires_at"]).isoformat()
        if payload.get("strategy_version") == "1.3":
            from sigbot.strategies.mm_sig_v1.live.models import Settings

            policy = Settings.model_validate(json.loads(payload["note"])["settings"])
            if policy.server_ttl_seconds is not None:
                assert self.approval is not None
                return min(
                    utc_time(payload["created_at"]) + timedelta(seconds=policy.server_ttl_seconds),
                    self.approval.new_orders_until,
                ).isoformat()
        return None

    def prepare(self, s, account, intent, order):
        inst = self.approve(s, account, order, "SUBMIT")
        assert self.approval is not None
        self.contract.require_documented_execution("submit_limit")
        payload = documented_limit_payload(
            inst.exchange_id,
            account.tournament,
            inst.outcome.lower(),
            order.side.lower(),
            order.quantity,
            order.price,
            order.id,
        )
        expiry = self.limit_expiration(intent.model_dump(mode="json"))
        if expiry:
            payload["expirationDate"] = expiry
            self.schema.validate("#/components/schemas/SingleOrderInput", payload)
        frozen = json.loads(json.dumps(payload, default=str))
        if s.get(LiveWire, order.id):
            raise Conflict("LIVE_REQUEST_ALREADY_PREPARED")
        s.add(
            LiveWire(
                order_id=order.id,
                approval_id=self.approval.approval_id,
                approval_digest=content_hash(self.approval.model_dump(mode="json")),
                payload=frozen,
                digest=content_hash(frozen),
            )
        )

    def before_cancel(self, s, account, order):
        self.approve(s, account, order, "CANCEL")
        if s.get(LiveWire, order.id) is None or order.remote_id is None:
            raise AuthorizationRequired("BOT_TRACKED_CANCEL_SCOPE_REQUIRED")
        self.contract.require_documented_execution("cancel_one")

    def call(self, name, **kwargs):
        return self.reader.call(name, auth_value="Bearer " + self.key, **kwargs)

    def charge_read(self, category, weight):
        import time

        # Renew the invocation bound, never the shared minute limit/backoff.
        with self.read_budget_lock:
            if (
                self.budget.count >= self.budget.max_requests
                or time.monotonic() >= self.budget.deadline
            ):
                self.budget = ReadBudget(self.budget.path, 80, 600)
            try:
                self.budget.charge(category, weight, wait=True)
            except RateLimited as exc:
                if exc.code != "READ_SESSION_LIMIT_REACHED":
                    raise
                self.budget = ReadBudget(self.budget.path, 80, 600)
                self.budget.charge(category, weight, wait=True)

    def pages(self, name, **kwargs):
        rows, count = [], 0
        if not hasattr(self, "history_coverage"):
            self.history_coverage: dict[str, list[dict | None]] = {}
        self.history_coverage[name] = []
        for page in self.reader.iter_pages(name, auth_value="Bearer " + self.key, **kwargs):
            count += 1
            if count > 100:
                raise SchemaMismatch("LIVE_HISTORY_PAGE_BOUND_REACHED")
            rows.extend(page["data"])
            self.history_coverage[name].append(page.get("coverage"))
        return rows, count

    def check(self, name, fn):
        return fn()

    @property
    def slug(self):
        return self.config.expected_tournament_slug

    def _write(self, name, logical_id, remote_id=None, payload=None):
        try:
            cap, body = self._prepare_write(name, logical_id, payload)
        except (
            AuthorizationRequired,
            ContractNotVerified,
            Conflict,
            SchemaMismatch,
            RateLimited,
        ) as exc:
            raise SubmissionNotSent(exc.code) from None
        with self.store.transaction() as s:
            self.store.lock_account(s, self.account_id)
            self.store.audit(
                s,
                self.account_id,
                "HTTP_TRANSMISSION_STARTED",
                {"order": logical_id, "capability": name},
                self.clock.now,
            )
        return self._transmit_write(name, logical_id, remote_id, cap, body)

    def _prepare_write(self, name, logical_id, payload):
        self.check_write_lock()
        if not self.mock:
            from datetime import datetime, timezone

            self.clock.now = datetime.now(timezone.utc)
        self.authorized()
        if self.execution_until and self.clock.now >= self.execution_until:
            raise AuthorizationRequired("AUTOMATIC_RUN_DEADLINE_REACHED")
        cap = self.contract.require_documented_execution(name)
        with self.store.transaction() as s:
            account = self.store.lock_account(s, self.account_id)
            order = s.get(Order, logical_id)
            if order is None:
                raise Conflict("LIVE_ORDER_SCOPE_MISMATCH")
            self.approve(s, account, order, "SUBMIT" if name == "submit_limit" else "CANCEL")
            if name == "submit_limit":
                from sigbot.storage import IntentRow

                intent = s.get(IntentRow, order.intent_id)
                if self.clock.now >= utc_time(intent.payload["expires_at"]):
                    raise AuthorizationRequired("LIVE_INTENT_EXPIRED_BEFORE_TRANSMISSION")
        self.budget.charge_probe_write()  # Same persisted account budget as the observer/probes.
        body = None
        if payload is not None:
            exact = {**payload, "price": Decimal(payload["price"])}
            self.schema.validate("#/components/schemas/SingleOrderInput", exact)
            body = exact_body(exact)
        return cap, body

    def _transmit_write(self, name, logical_id, remote_id, cap, body, *, recovery=False):
        self.call_count[cap["http_method"]] += 1
        started_at, started_monotonic = self.clock.sync().isoformat(), time.monotonic()
        try:
            response = self.writer.request(
                cap["http_method"],
                OFFICIAL_ORIGIN + cap["path"].replace("{id}", str(remote_id)),
                headers={"Authorization": "Bearer " + self.key, "Content-Type": "application/json"},
                content=body,
            )
        except httpx.HTTPError:
            with self.store.transaction() as s:
                self.store.audit(
                    s,
                    self.account_id,
                    "HTTP_TRANSPORT_UNCONFIRMED",
                    {
                        "order": logical_id,
                        "capability": name,
                        "http_started_at": started_at,
                        "http_elapsed_seconds": str(time.monotonic() - started_monotonic),
                    },
                    self.clock.sync(),
                )
            raise TransportUnknown("LIVE_WRITE_RESULT_UNKNOWN") from None
        response_received_at = self.clock.sync().isoformat()
        http_elapsed = str(time.monotonic() - started_monotonic)
        # Store private sanitized bytes even when decoding fails; never log remote errors.
        raw = response.content.replace(self.key.encode(), b"[REDACTED]")
        digest = hashlib.sha256(raw).hexdigest()
        p = Path(self.config.raw_output_dir) / (name + "-" + logical_id + "-" + digest + ".json")
        if not p.exists():
            private_write(p, raw)
        if response.status_code in {429, 503}:
            retry = response.headers.get("Retry-After", "60")
            self.budget.backoff(int(retry) if retry.isdecimal() else 60)
        if name == "submit_limit" and response.status_code in {400, 401, 403}:
            try:
                rejected = json.loads(raw, parse_float=Decimal)
                self.schema.validate(
                    "#/paths/"
                    + pointer(cap["openapi_path"])
                    + "/post/responses/"
                    + str(response.status_code)
                    + "/content/application~1json/schema",
                    rejected,
                )
            except (ValueError, UnicodeError, SchemaMismatch):
                raise TransportUnknown("LIVE_WRITE_RESULT_UNKNOWN") from None
            with self.store.transaction() as s:
                account = self.store.lock_account(s, self.account_id)
                wire = s.get(LiveWire, logical_id)
                if not recovery:
                    wire.receipt = {
                        "placement_rejected": True,
                        "http_status": response.status_code,
                        "body_sha256": digest,
                    }
                self.store.audit(
                    s,
                    account.id,
                    "LIVE_RECOVERY_REJECTION_UNRESOLVED" if recovery else "LIVE_REJECTION_RECEIPT",
                    {
                        "order": logical_id,
                        "http_status": response.status_code,
                        "body_sha256": digest,
                    },
                    self.clock.now,
                )
            if recovery:
                # Retention is not documented. A new rejection cannot disprove
                # an earlier accepted placement whose response was lost.
                raise TransportUnknown("LIVE_RECOVERY_REJECTION_UNRESOLVED")
            raise OrderRejected("OFFICIAL_PLACEMENT_REJECTED")
        if name == "submit_limit" and response.status_code == 409:
            try:
                rejected = json.loads(raw, parse_float=Decimal)
                self.schema.validate(
                    "#/paths/"
                    + pointer(cap["openapi_path"])
                    + "/post/responses/409/content/application~1json/schema",
                    rejected,
                )
            except (ValueError, UnicodeError, SchemaMismatch):
                raise TransportUnknown("LIVE_WRITE_RESULT_UNKNOWN") from None
            if rejected.get("error", {}).get("code") == "IDEMPOTENCY_KEY_EXPIRED":
                with self.store.transaction() as s:
                    self.store.lock_account(s, self.account_id)
                    self.store.audit(
                        s,
                        self.account_id,
                        "LIVE_RECOVERY_REJECTION_UNRESOLVED",
                        {
                            "order": logical_id,
                            "http_status": 409,
                            "body_sha256": digest,
                            "code": "IDEMPOTENCY_KEY_EXPIRED",
                            "reservation_released": False,
                        },
                        self.clock.now,
                    )
                # Losing the replay does not prove the original request failed.
                # Keep the frozen key, original receipt and full risk occupation.
                raise TransportUnknown("LIVE_IDEMPOTENCY_REPLAY_EXPIRED_UNRESOLVED")
        if response.status_code != 200 or self.key.encode() in response.content:
            raise TransportUnknown("LIVE_WRITE_RESULT_UNKNOWN")
        try:
            data = json.loads(
                raw,
                parse_float=Decimal,
                parse_constant=lambda _: (_ for _ in ()).throw(ValueError("nonfinite JSON")),
            )
        except (ValueError, UnicodeError):
            raise TransportUnknown("LIVE_WRITE_RESULT_UNKNOWN") from None
        try:
            self.schema.response(cap["openapi_path"], cap["http_method"].lower(), data)
        except SchemaMismatch:
            if name == "submit_limit":
                self.retain_no_query_hint(logical_id, cap, data, digest)
            # The hint does not accept the invalid POST, create fills or release risk.
            raise TransportUnknown("LIVE_WRITE_RESULT_UNKNOWN") from None
        with self.store.transaction() as s:
            account = self.store.lock_account(s, self.config.expected_account_id)
            order = s.get(Order, logical_id)
            wire = s.get(LiveWire, logical_id)
            if order is None or wire is None:
                raise Conflict("LIVE_REQUEST_MISSING")
            oid = data.get("orderId")
            if recovery and oid is None:
                raise TransportUnknown("LIVE_RECOVERY_NO_REMOTE_ID_UNRESOLVED")
            if (
                oid is None
                and name == "submit_limit"
                and data["exchangeId"] == wire.payload["exchangeId"]
                and not data["open"]
                and Decimal(str(data["quantityTraded"])) == ZERO
                and Decimal(str(data["totalCost"])) == ZERO
                and data["fillPrice"] is None
            ):
                wire.receipt = {
                    "placement_rejected": True,
                    "no_order_noop": True,
                    "body_sha256": digest,
                }
                self.store.audit(
                    s, account.id, "OFFICIAL_NO_ORDER_NOOP", {"order": logical_id}, self.clock.now
                )
            elif oid is None or (order.remote_id is not None and order.remote_id != str(oid)):
                raise TransportUnknown("LIVE_REMOTE_ID_UNKNOWN_OR_CHANGED")
            elif name == "submit_limit":
                wire.receipt = json.loads(json.dumps(data, default=str))
                order.remote_id = str(oid)
            elif (
                str(oid) != order.remote_id
                or data["tournamentId"] != self.config.expected_tournament_id
            ):
                raise SchemaMismatch("CANCEL_RECEIPT_SCOPE_MISMATCH")
            self.store.audit(
                s,
                order.account_id,
                "LIVE_RECEIPT",
                {
                    "order": logical_id,
                    "capability": name,
                    "body_sha256": digest,
                    "http_status": 200,
                    "http_started_at": started_at,
                    "response_received_at": response_received_at,
                    "http_elapsed_seconds": http_elapsed,
                },
                self.clock.now,
            )

        if oid is None:
            raise OrderRejected("OFFICIAL_NO_ORDER_NOOP")
        return data

    def retain_no_query_hint(self, logical_id, cap, data, digest):
        """Quarantine a known scalar anomaly; only documented GETs can resolve it."""
        errors = self.schema.response_error_paths(cap["openapi_path"], "post", data)
        if errors != [(("remainingQuantity",), "minimum")]:
            return False
        with self.store.transaction() as s:
            account = self.store.lock_account(s, self.account_id)
            order, wire = s.get(Order, logical_id), s.get(LiveWire, logical_id)
            row = s.get(IntentRow, order.intent_id) if order else None
            if (
                row is None
                or wire is None
                or (row.payload.get("strategy_id"), row.payload.get("strategy_version"))
                not in {
                    ("ssr_sig", "1.0"),
                    ("mm_sig", "1.3"),
                    ("grid_sig", "1.0"),
                    ("grid_sig", "2.0"),
                }
            ):
                return False
            payload, oid = wire.payload, data.get("orderId")
            economics = data.get("all")
            position = s.get(Position, (account.id, order.instrument_id))
            covered_sell = (
                payload["action"] == "sell"
                and order.side == "SELL"
                and not order.filled
                and not order.observed_filled
                and order.reserve_quantity == order.quantity
                and position is not None
                and position.quantity >= order.quantity
            )
            if (
                isinstance(oid, bool)
                or not isinstance(oid, int)
                or oid <= 0
                or payload["side"] != "no"
                or payload["action"] != "buy"
                and not covered_sell
                or order.side.lower() != payload["action"]
                or any(
                    data.get(k) != payload[k] for k in ("exchangeId", "side", "action", "quantity")
                )
                or Decimal(str(data.get("price"))) != Decimal(payload["price"])
                or Decimal(str(data["remainingQuantity"]))
                != -(Decimal(payload["quantity"]) - Decimal(str(data["quantityTraded"])))
                or not isinstance(economics, dict)
                or not {
                    "collateralSavings",
                    "outstandingAdvanceAfter",
                    "collateralRepayment",
                    "guaranteedPayoutFloorAfter",
                    "redemptionCredit",
                    "relationshipIds",
                    "componentId",
                }
                <= set(economics)
                or any(
                    economics.get(k) != ZERO
                    for k in (
                        "collateralSavings",
                        "outstandingAdvanceAfter",
                        "collateralRepayment",
                        "guaranteedPayoutFloorAfter",
                        "redemptionCredit",
                    )
                )
                or economics.get("relationshipIds") != []
                or economics.get("componentId") is not None
                or order.remote_id not in {None, str(oid)}
            ):
                return False
            order.remote_id = str(oid)
            wire.receipt = {
                **json.loads(json.dumps(data, default=str)),
                "POST_schema_status": "REJECTED_QUERY_REQUIRED",
            }
            self.store.audit(
                s,
                account.id,
                "SSR_INVALID_POST_QUERY_HINT"
                if row.payload.get("strategy_id") == "ssr_sig"
                else "GRID_INVALID_POST_QUERY_HINT"
                if row.payload.get("strategy_id") == "grid_sig"
                else "MM_INVALID_POST_QUERY_HINT",
                {
                    "order": logical_id,
                    "remote_id": str(oid),
                    "body_sha256": digest,
                    "POST_schema_accepted": False,
                    "fills_created": 0,
                    "reservation_released": False,
                },
                self.clock.now,
            )
            return True

    def recover_expired_submission(self, logical_id):
        """Replay only an expired MM limit's exact frozen identity, under the OMS claim.

        A successful identified receipt can resolve the original. Absence, a
        rejection, or a no-op cannot release risk. No expiry/key/payload renewal.
        """
        self.check_write_lock()
        self.clock.sync()
        cap = self.contract.require_documented_execution("submit_limit")
        description = self.schema.spec["paths"]["/orders"]["post"].get("description", "")
        expiry_description = self.schema.spec["components"]["schemas"]["OrderInput"]["properties"][
            "expirationDate"
        ].get("description", "")
        if (
            not cap.get("idempotency", {}).get("supported")
            or "replays the exact stored response before mutable settlement or market preflights"
            not in description
            or "future ISO 8601 datetime" not in expiry_description
        ):
            raise ContractNotVerified("EXPIRED_SUBMISSION_REPLAY_CONTRACT_REQUIRED")
        with self.store.transaction() as s:
            account = self.store.lock_account(s, self.account_id)
            order, wire = s.get(Order, logical_id), s.get(LiveWire, logical_id)
            if order is None or wire is None or order.account_id != account.id:
                raise Conflict("LIVE_ORDER_SCOPE_MISMATCH")
            row = s.get(IntentRow, order.intent_id)
            expiry = wire.payload.get("expirationDate")
            if (
                order.remote_id
                or wire.receipt
                or order.request_state not in {"SENDING", "UNKNOWN"}
                or not row
                or row.payload.get("strategy_id") != "mm_sig"
                or row.payload.get("strategy_version") != "1.3"
                or not expiry
                or self.clock.now < utc_time(expiry) + timedelta(seconds=90)
            ):
                return False
            inst = s.get(Instrument, order.instrument_id)
            expected = documented_limit_payload(
                inst.exchange_id,
                account.tournament,
                inst.outcome.lower(),
                order.side.lower(),
                order.quantity,
                order.price,
                logical_id,
            )
            expected["expirationDate"] = self.limit_expiration(row.payload)
            # PostgreSQL pads Decimal columns; compare the economic price while
            # preserving the original wire spelling and digest for the replay.
            expected["price"] = wire.payload.get("price")
            if (
                Decimal(expected["price"]) != order.price
                or expected != wire.payload
                or content_hash(wire.payload) != wire.digest
            ):
                raise Conflict("LIVE_FROZEN_REQUEST_CHANGED")
            transmitted = s.scalar(
                select(Audit.id)
                .where(
                    Audit.account_id == account.id,
                    Audit.kind == "HTTP_TRANSMISSION_STARTED",
                    Audit.data["order"].as_string() == logical_id,
                )
                .limit(1)
            )
            retries = list(
                s.scalars(
                    select(Audit)
                    .where(
                        Audit.account_id == account.id,
                        Audit.kind == "EXPIRED_SUBMISSION_REPLAY_STARTED",
                        Audit.data["order"].as_string() == logical_id,
                    )
                    .order_by(Audit.at)
                )
            )
            rejected = s.scalar(
                select(Audit.id)
                .where(
                    Audit.account_id == account.id,
                    Audit.kind == "LIVE_RECOVERY_REJECTION_UNRESOLVED",
                    Audit.data["order"].as_string() == logical_id,
                )
                .limit(1)
            )
            if (
                not transmitted
                or rejected
                or len(retries) >= 3
                or retries
                and (self.clock.now - retries[-1].at).total_seconds() < 300
            ):
                return False
            self.approve(s, account, order, "SUBMIT")
            self.budget.charge_probe_write()
            exact = {**wire.payload, "price": Decimal(wire.payload["price"])}
            self.schema.validate("#/components/schemas/SingleOrderInput", exact)
            body = exact_body(exact)
            self.store.audit(
                s,
                account.id,
                "EXPIRED_SUBMISSION_REPLAY_STARTED",
                {"order": logical_id, "wire_digest": wire.digest, "expiration_unchanged": expiry},
                self.clock.now,
            )
        self._transmit_write("submit_limit", logical_id, None, cap, body, recovery=True)
        return True

    def submit_limit(self, request):
        if (
            request.synthetic
            or request.account_id != self.config.expected_account_id
            or request.tournament_id != self.config.expected_tournament_id
        ):
            raise AuthorizationRequired("SYNTHETIC_REAL_TRANSPORT_FORBIDDEN")
        with self.store.transaction() as s:
            order, wire = s.get(Order, request.logical_id), s.get(LiveWire, request.logical_id)
            if (
                order is None
                or wire is None
                or order.request_state != "SENDING"
                or wire.receipt is not None
            ):
                raise Conflict("LIVE_ONCE_PREPARED_REQUEST_REQUIRED")
            inst = s.get(Instrument, request.instrument_id)
            expected = documented_limit_payload(
                inst.exchange_id,
                request.tournament_id,
                inst.outcome.lower(),
                request.side.lower(),
                request.quantity,
                request.price,
                request.logical_id,
            )
            from sigbot.storage import IntentRow

            intent_row = s.get(IntentRow, order.intent_id)
            expiry = self.limit_expiration(intent_row.payload)
            if expiry:
                expected["expirationDate"] = expiry
                self.schema.validate("#/components/schemas/SingleOrderInput", expected)
            if content_hash(expected) != wire.digest:
                raise Conflict("LIVE_FROZEN_REQUEST_CHANGED")
            frozen = dict(wire.payload)
        try:
            receipt = self._write("submit_limit", request.logical_id, payload=frozen)
        except TransportUnknown:
            with self.store.transaction() as s:
                row = s.get(IntentRow, s.get(Order, request.logical_id).intent_id)
                wire = s.get(LiveWire, request.logical_id)
                query_known_no = bool(
                    (row.payload.get("strategy_id"), row.payload.get("strategy_version"))
                    in {("mm_sig", "1.3"), ("grid_sig", "1.0"), ("grid_sig", "2.0")}
                    and wire.receipt
                    and wire.receipt.get("POST_schema_status") == "REJECTED_QUERY_REQUIRED"
                )
            if not query_known_no:
                raise
            # The rejected POST is only an identity hint. The normal full GET
            # proof supplies the acknowledgement; a failed read remains unknown.
            observation = self.get_order(request.logical_id)
            if observation is None:
                raise TransportUnknown("LIVE_ACCEPTED_REPORTING_PENDING") from None
            return observation
        observation = self.accepted_zero_fill_receipt(request.logical_id, receipt)
        if observation is not None:
            return observation
        observation = self.get_order(request.logical_id)
        if observation is None:
            raise TransportUnknown("LIVE_ACCEPTED_REPORTING_PENDING")
        return observation

    def accepted_zero_fill_receipt(self, logical_id, receipt):
        """A fresh engine receipt proves a resting, completely unfilled order.

        This is admission evidence, not a fill, closure, or an account snapshot.
        All original reserves survive; the complete periodic query still polls it.
        """

        def exact(value, expected):
            try:
                return (
                    value is not None
                    and not isinstance(value, bool)
                    and Decimal(str(value)) == expected
                )
            except (InvalidOperation, ValueError, TypeError):
                return False

        with self.store.transaction() as s:
            order = s.get(Order, logical_id)
            row = s.get(IntentRow, order.intent_id) if order else None
            wire = s.get(LiveWire, logical_id)
            identity = (
                (row.payload.get("strategy_id"), row.payload.get("strategy_version"))
                if row
                else None
            )
            if identity not in {("mm_sig", "1.3"), ("grid_sig", "1.0"), ("grid_sig", "2.0")}:
                return None
            metadata = json.loads(row.payload.get("note") or "{}") if row else {}
            supported = (
                identity == ("mm_sig", "1.3")
                and metadata.get("policy", {}).get("calibration_sampling")
                or identity in {("grid_sig", "1.0"), ("grid_sig", "2.0")}
                and metadata.get("kind") in {"ENTRY", "EXIT", "RISK_EXIT"}
            )
            if row is None or wire is None or not supported:
                return None
            payload = wire.payload
            economics = receipt.get("all", {})
            if identity in {("grid_sig", "1.0"), ("grid_sig", "2.0")} and (
                not isinstance(economics, dict)
                or economics.get("relationshipIds") != []
                or economics.get("componentId") is not None
                or any(
                    not exact(economics.get(k), ZERO)
                    for k in (
                        "fullNotionalCost",
                        "effectiveEntryCost",
                        "collateralSavings",
                        "guaranteedPayoutFloorAfter",
                        "outstandingAdvanceAfter",
                        "collateralRepayment",
                        "netBuyingPowerImpact",
                        "redemptionCredit",
                    )
                )
            ):
                return None
            if (
                receipt.get("open") is not True
                or str(receipt.get("orderId")) != order.remote_id
                or receipt.get("exchangeId") != payload["exchangeId"]
                or receipt.get("side") != payload["side"]
                or receipt.get("action") != payload["action"]
                or not exact(receipt.get("price"), order.price)
                or not exact(receipt.get("quantity"), order.quantity)
                or not exact(receipt.get("remainingQuantity"), order.quantity)
                or not exact(receipt.get("quantityTraded"), ZERO)
                or not exact(receipt.get("totalCost"), ZERO)
                or receipt.get("fillPrice") is not None
                or not economics
                or any(
                    not exact(economics.get(k), ZERO)
                    for k in ("collateralSavings", "outstandingAdvanceAfter", "collateralRepayment")
                )
            ):
                return None
            self.store.lock_account(s, order.account_id)
            signature = content_hash(["AUTHORITATIVE_ZERO_FILL_PLACEMENT_RECEIPT", receipt])
            if wire.observation_digest != signature:
                wire.projection_revision += 1
                wire.observation_digest = signature
            self.store.audit(
                s,
                order.account_id,
                "GRID_ACCEPTED_ZERO_FILL_RECEIPT"
                if identity in {("grid_sig", "1.0"), ("grid_sig", "2.0")}
                else "MM_ACCEPTED_ZERO_FILL_RECEIPT",
                {
                    "order": logical_id,
                    "remote_id": order.remote_id,
                    "fills_created": 0,
                    "reservation_release": False,
                },
                self.clock.now,
            )
            return Observation(
                order.remote_id,
                logical_id,
                order.instrument_id,
                order.side,
                order.quantity,
                order.price,
                ZERO,
                "OPEN",
                wire.projection_revision,
            )

    def recover_archived_no_hint(self, logical_id):
        """Recover this request's original response identity; GET must still prove it.

        No POST, fill, acknowledgement or reserve release. Never match an order
        by price/time/size or choose among multiple archived response bodies.
        """
        with self.store.transaction() as session:
            order = session.get(Order, logical_id)
            wire = session.get(LiveWire, logical_id)
            row = session.get(IntentRow, order.intent_id) if order else None
            if (
                not order
                or not wire
                or not row
                or order.account_id != self.account_id
                or order.remote_id
                or wire.receipt
                or order.request_state not in {"SENDING", "UNKNOWN"}
                or (row.payload.get("strategy_id"), row.payload.get("strategy_version"))
                not in {("grid_sig", "1.0"), ("grid_sig", "2.0")}
                or wire.payload.get("side") != "no"
                or wire.payload.get("action") not in {"buy", "sell"}
                or content_hash(wire.payload) != wire.digest
            ):
                return False
            sent = session.scalar(
                select(Audit.id)
                .where(
                    Audit.account_id == order.account_id,
                    Audit.kind == "HTTP_TRANSMISSION_STARTED",
                    Audit.data["order"].as_string() == logical_id,
                )
                .limit(1)
            )
            if not sent:
                return False
        paths = list(
            Path(self.config.raw_output_dir).glob("submit_limit-" + logical_id + "-*.json")
        )
        if len(paths) != 1:
            return False
        raw = paths[0].read_bytes()
        digest = hashlib.sha256(raw).hexdigest()
        if paths[0].name != "submit_limit-" + logical_id + "-" + digest + ".json":
            return False
        try:
            data = json.loads(
                raw,
                parse_float=Decimal,
                parse_constant=lambda _: (_ for _ in ()).throw(ValueError("nonfinite JSON")),
            )
        except (ValueError, UnicodeError):
            return False
        if not isinstance(data, dict):
            return False
        cap = self.contract.require_documented_execution("submit_limit")
        return self.retain_no_query_hint(logical_id, cap, data, digest)

    def get_order(self, logical_id):
        self.recover_archived_no_hint(logical_id)
        try:
            return self._get_order(logical_id)
        except HistoryReadError as exc:
            if exc.code != "ORDER_HISTORY_ARCHIVED":
                raise
            with self.store.transaction() as s:
                self.store.lock_account(s, self.account_id)
                order, wire = s.get(Order, logical_id), s.get(LiveWire, logical_id)
                if (
                    order is None
                    or wire is None
                    or order.account_id != self.account_id
                    or order.state not in TERMINAL
                    or order.request_state != "RESOLVED"
                    or order.filled
                    or order.observed_filled
                    or order.reserve_cash
                    or order.reserve_quantity
                    or order.cancel_state in {"CANCEL_REQUESTED", "CANCEL_UNKNOWN"}
                    or not wire.observation_digest
                    or wire.projection_revision < 1
                    or s.scalar(select(FillRow.id).where(FillRow.order_id == logical_id).limit(1))
                ):
                    raise TransportUnknown("ARCHIVED_ORDER_REQUIRES_LOCAL_TERMINAL_PROOF") from None
                self.store.audit(
                    s,
                    self.account_id,
                    "OFFICIAL_ORDER_HISTORY_ARCHIVED",
                    {
                        "order": logical_id,
                        "official_order_id": order.remote_id,
                        "existing_terminal_proof": wire.observation_digest,
                        "economic_state_changed": False,
                    },
                    self.clock.now,
                )
                return Observation(
                    order.remote_id,
                    logical_id,
                    order.instrument_id,
                    order.side,
                    order.quantity,
                    order.price,
                    ZERO,
                    order.state,
                    wire.projection_revision,
                )

    def _get_order(self, logical_id):
        with self.store.transaction() as s:
            order, wire = s.get(Order, logical_id), s.get(LiveWire, logical_id)
            if order is None or wire is None or order.account_id != self.config.expected_account_id:
                raise Conflict("LIVE_ORDER_SCOPE_MISMATCH")
            if order.remote_id is None:
                return None  # No documented lookup by idempotency key; never infer by time/amount.
            remote, request = order.remote_id, dict(wire.payload)
            receipt = dict(wire.receipt or {})
        economics = receipt.get("all")
        if receipt and (
            receipt.get("exchangeId") != request["exchangeId"]
            or receipt.get("action", request["action"]) != request["action"]
            or receipt.get("side", request["side"]) != request["side"]
            or Decimal(str(receipt.get("price", request["price"]))) != Decimal(request["price"])
            or Decimal(str(receipt.get("quantity", request["quantity"])))
            > Decimal(request["quantity"])
        ):
            raise SchemaMismatch("UNSUPPORTED_SIG_ORDER_NORMALIZATION")
        if economics and any(
            Decimal(str(economics.get(k, "0"))) != ZERO
            for k in ("collateralSavings", "outstandingAdvanceAfter", "collateralRepayment")
        ):
            raise SchemaMismatch("SIG_COLLATERAL_ECONOMICS_UNSUPPORTED")
        first = self.call("order_detail", path_params={"id": remote})
        fills, cursors, totals, sequence = [], set(), None, None
        query = {"limit": 200}
        for _ in range(100):
            page = self.call("order_fills", path_params={"id": remote}, query=query)
            if (
                str(page["orderId"]) != remote
                or page["exchangeId"] != request["exchangeId"]
                or page["tournamentId"] != request["tournamentId"]
                or not page["coverage"]["complete"]
            ):
                raise SchemaMismatch("LIVE_FILL_SCOPE_OR_COVERAGE_MISMATCH")
            total = Decimal(str(page["totalQuantityFilled"]))
            seq = page["coverage"]["projectedThroughSequence"]
            if totals is not None and (totals, sequence) != (total, seq):
                raise TransportUnknown("LIVE_FILL_PAGINATION_CHANGED")
            totals, sequence = total, seq
            fills.extend(page["data"])
            pg = page["pagination"]
            if not pg["hasMore"]:
                if pg["nextCursor"] is not None:
                    raise SchemaMismatch("LIVE_FILL_CURSOR_CONFLICT")
                break
            cursor = pg["nextCursor"]
            if not cursor or cursor in cursors or not page["data"]:
                raise SchemaMismatch("LIVE_FILL_CURSOR_LOOP")
            cursors.add(cursor)
            query["cursor"] = cursor
        else:
            raise SchemaMismatch("LIVE_FILL_PAGE_BOUND_REACHED")
        second = self.call("order_detail", path_params={"id": remote})
        if content_hash(first) != content_hash(second):
            raise TransportUnknown("LIVE_ORDER_READ_WINDOW_CHANGED")
        if (
            str(second["id"]),
            second["exchangeId"],
            second["tournamentId"],
            second["side"],
            second["action"],
            second["priceLimit"],
        ) != (
            remote,
            request["exchangeId"],
            request["tournamentId"],
            request["side"],
            request["action"],
            Decimal(request["price"]),
        ):
            raise SchemaMismatch("UNSUPPORTED_SIG_ORDER_NORMALIZATION")
        assert totals is not None
        expected_positive = request["side"] == "yes"
        if totals and (totals > 0) != expected_positive:
            raise SchemaMismatch("LIVE_FILL_OUTCOME_MISMATCH")
        q, cumulative = Decimal(request["quantity"]), abs(totals)
        if (
            cumulative > q
            or sum((abs(Decimal(str(f["quantity"]))) for f in fills), ZERO) != cumulative
        ):
            raise SchemaMismatch("LIVE_CUMULATIVE_FILL_MISMATCH")
        reported = Decimal(str(second["quantity"]))
        if reported < 0 or reported + (cumulative if second["open"] else ZERO) > q:
            raise SchemaMismatch("LIVE_REPORTED_QUANTITY_MISMATCH")
        if self.fee is None:
            raise ContractNotVerified("EXECUTION_FEE_EVIDENCE_REQUIRED")
        self.fee.check(self.mock)
        normalized, seen = [], set()
        for f in sorted(fills, key=lambda x: (x["filledAt"], x["id"])):
            signed = Decimal(str(f["quantity"]))
            fid = "SIG-FILL-" + content_hash(
                [request["tournamentId"], remote, f["id"], f["filledAt"], f["side"]]
            )
            if (
                fid in seen
                or not signed
                or (signed > 0) != expected_positive
                or f["side"] != request["side"]
                or f["price"] is None
            ):
                raise SchemaMismatch("LIVE_DUPLICATE_OR_UNKNOWN_FILL")
            seen.add(fid)
            normalized.append(
                Fill(
                    fid,
                    remote,
                    order.instrument_id,
                    order.side,
                    abs(signed),
                    Decimal(str(f["price"])),
                    abs(signed) * self.fee.per_share,
                    utc_time(f["filledAt"]),
                )
            )
        # Closed is proven; the API has no general cancelled-vs-expired status field.
        state = (
            ("PARTIAL" if cumulative else "OPEN")
            if second["open"]
            else ("FILLED" if cumulative == q else "CLOSED")
        )
        signature = content_hash([second, [asdict(f) for f in normalized]])
        with self.store.transaction() as s:
            self.store.lock_account(s, order.account_id)
            wire = s.get(LiveWire, logical_id)
            if wire.observation_digest != signature:
                wire.projection_revision += 1
                wire.observation_digest = signature
            revision = wire.projection_revision
        return Observation(
            remote,
            logical_id,
            order.instrument_id,
            order.side,
            q,
            order.price,
            cumulative,
            state,
            revision,
            tuple(normalized),
        )

    def cancel_order(self, remote_id):
        with self.store.transaction() as s:
            order = s.scalar(
                select(Order).where(
                    Order.account_id == self.config.expected_account_id,
                    Order.remote_id == remote_id,
                )
            )
            if order is None or order.cancel_state != "CANCEL_REQUESTED":
                raise AuthorizationRequired("BOT_TRACKED_CANCEL_SCOPE_REQUIRED")
            logical_id = order.id
        self._write("cancel_one", logical_id, remote_id=remote_id)
        observation = self.get_order(logical_id)
        if observation is None:
            raise TransportUnknown("LIVE_CANCEL_REPORTING_PENDING")
        return observation

    def snapshot(self):
        from sigbot.shared_marketdata import account_read_priority

        with account_read_priority(self.config.expected_account_id, self.tournament_id):
            result = self._snapshot()
            self.clock.sync()
            return result

    def _snapshot(self):
        self.reader.identity("Bearer " + self.key)
        previous = (
            getattr(self, "last_account_data", None)
            if (getattr(self, "last_account_check", None) or {}).get("status") == "PASS"
            else None
        )
        if previous is not None and (
            not previous.get("pagination_complete") or previous["cup"]["id"] != self.tournament_id
        ):
            previous = None
        result, data = checkpoint(self, previous=previous)
        positions: dict[str, tuple[Decimal, Decimal | None]] = {}
        observations: list[Observation] = []
        with self.store.transaction() as s:
            instruments = list(
                s.scalars(
                    select(Instrument).where(
                        Instrument.tournament == self.config.expected_tournament_id
                    )
                )
            )
            lookup = {i.exchange_id + ":" + i.outcome: i.id for i in instruments if i.exchange_id}
            tracked = list(
                s.scalars(select(Order).where(Order.account_id == self.config.expected_account_id))
            )
            by_id = {i.id: i for i in instruments}
            # Strategy-specific aliases cannot replace the instrument that owns
            # an actual position or an outstanding real order.
            for order in tracked:
                if order.instrument_id in by_id and (
                    order.state not in TERMINAL or order.reserve_cash or order.reserve_quantity
                ):
                    inst = by_id[order.instrument_id]
                    lookup[inst.exchange_id + ":" + inst.outcome] = inst.id
            owned_keys = set()
            for pos in s.scalars(select(Position).where(Position.account_id == self.account_id)):
                if not pos.quantity:
                    continue
                inst = by_id[pos.instrument_id]
                key = inst.exchange_id + ":" + inst.outcome
                if key in owned_keys:
                    raise SchemaMismatch("MULTIPLE_POSITION_ALIASES_FOR_ONE_REAL_ASSET")
                owned_keys.add(key)
                lookup[key] = inst.id
        for key, pos in result["positions"].items():
            if key not in lookup:
                raise SchemaMismatch("ACCOUNT_POSITION_MAPPING_REQUIRED")
            positions[lookup[key]] = (Decimal(pos["quantity"]), Decimal(pos["cost"]))
        remote_ids = {o.remote_id for o in tracked}
        if any(o["open"] and str(o["id"]) not in remote_ids for o in data["orders"]):
            raise SchemaMismatch("EXTERNAL_OPEN_ORDER_IMPORT_REQUIRED")
        for order in tracked:
            if order.remote_id and (
                order.request_state != "RESOLVED"
                or order.state not in TERMINAL
                or order.reserve_cash
                or order.reserve_quantity
                or order.cancel_state in {"CANCEL_REQUESTED", "CANCEL_UNKNOWN"}
            ):
                obs = self.get_order(order.id)
                if obs is not None:
                    observations.append(obs)
        self.last_account_check = result
        self.last_account_data = data
        opening_lots = {}
        for pos in data["positions"]["positions"]:
            if not pos["quantity"]:
                continue
            key = pos["exchangeId"] + (":YES" if pos["quantity"] > 0 else ":NO")
            if key in lookup:
                opening_lots[lookup[key]] = pos["lots"]
        return AccountSnapshot(
            self.account_id,
            self.tournament_id,
            Decimal(result["projected_cash"]),
            positions,
            observations,
            result["checkpoint"],
            coherent=result["status"] == "PASS",
            position_lots=opening_lots,
        )

    def close(self):
        self.release_write_lock()
        self.reader.close()
        self.writer.close()
        self.key = ""

    def refresh_book(self, instrument_id):
        """Read complete own orders before excluding their quantities from depth."""
        own, _ = self.pages(
            "open_orders",
            query={"tournamentId": self.tournament_id, "status": "open", "limit": 200},
        )
        with self.store.transaction() as s:
            inst = s.get(Instrument, instrument_id)
            if inst is None or inst.tournament != self.tournament_id or inst.exchange_id is None:
                raise SchemaMismatch("REAL_INSTRUMENT_MAPPING_REQUIRED")
            eid, market_id = inst.exchange_id, inst.market_id
            market = s.get(Market, market_id)
            api_mid = market.api_id
            expected_nodes = market.raw.get("reviewed_sig_nodes")
        detail = self.call(
            "market_detail", path_params={"id": api_mid}, query={"tournamentId": self.tournament_id}
        )
        contexts = [
            c
            for c in detail["contexts"]
            if c.get("type") == "tournament"
            and c.get("tournament", {}).get("id") == self.tournament_id
        ]
        if (
            detail["id"] != api_mid
            or len(contexts) != 1
            or contexts[0]["status"] != "open"
            or eid not in {x["id"] for x in contexts[0]["exchanges"]}
        ):
            raise SchemaMismatch("LIVE_MARKET_STATUS_OR_MAPPING_CHANGED")
        if expected_nodes is None and not self.mock:
            raise ContractNotVerified("CURRENT_SIG_RULE_REFERENCE_REQUIRED")
        if expected_nodes is not None:
            try:
                nodes = self.call(
                    "market_nodes",
                    path_params={"id": api_mid},
                    query={"tournamentId": self.tournament_id},
                )
            except SigbotError as exc:
                with self.store.transaction() as s:
                    self.store.audit(
                        s, self.account_id, "OFFICIAL_RULE_DOCUMENT_UNAVAILABLE",
                        {"policy": "REPORT_ONLY", "instrument": instrument_id,
                         "source": "market_nodes", "code": exc.code}, self.clock.now,
                    )
            else:
                if content_hash(nodes) != content_hash(expected_nodes):
                    with self.store.transaction() as s:
                        self.store.audit(
                            s, self.account_id, "OFFICIAL_RULE_DOCUMENT_CHANGE",
                            {"policy": "REPORT_ONLY", "instrument": instrument_id,
                             "source": "market_nodes", "old": expected_nodes, "new": nodes,
                             "old_hash": content_hash(expected_nodes),
                             "new_hash": content_hash(nodes)}, self.clock.now,
                        )
        data = self.call(
            "orderbook",
            path_params={"id": eid},
            query={"tournamentId": self.tournament_id, "depth": 200},
        )
        if (data["exchangeId"], data["marketId"]) != (eid, api_mid):
            raise SchemaMismatch("LIVE_BOOK_SCOPE_MISMATCH")
        if not self.mock:
            from datetime import datetime, timezone

            self.clock.now = datetime.now(timezone.utc)
        received = self.clock.now
        from sigbot.strategies.mm_sig_v1.data import normalized_book, outcome_book

        if any(o["tournamentId"] != self.tournament_id for o in own):
            raise SchemaMismatch("LIVE_OWN_ORDER_PRICE_OR_SCOPE_UNKNOWN")
        verified = normalized_book(data, own, received)
        with self.store.transaction() as s:
            # One complete official book is YES-normalized. Mirror its two
            # mapped outcomes atomically; never join two independent snapshots.
            for inst in s.scalars(select(Instrument).where(
                Instrument.tournament == self.tournament_id,
                Instrument.exchange_id == eid, Instrument.market_id == market_id,
            )):
                book = outcome_book(verified, inst.outcome)
                previous = inst.book.get("sequence")
                if previous is not None and book.sequence < int(previous):
                    raise SchemaMismatch("LIVE_BOOK_VERSION_REGRESSION")
                inst.book = {
                    **book.model_dump(mode="json", exclude={"received_at", "quality"}),
                    "validated_at": received.isoformat(),
                    # The common REST adapter historically publishes external
                    # quantities. Keep that contract for legacy consumers;
                    # subtract only after validating the gross/own bound.
                    **{side: [
                        {"price": str(level.price),
                         "quantity": str(level.quantity - level.own_quantity),
                         "own_quantity": "0"}
                        for level in getattr(book, side)
                        if level.quantity > level.own_quantity
                    ] for side in ("bids", "asks")},
                }
                inst.book_at, inst.quality = received, book.quality
                inst.mapping_evidence = {
                    **inst.mapping_evidence, "live_checked_at": received.isoformat(),
                    "live_book_validation_source": "FULL_REST_BOOK_AND_COMPLETE_OWN_ORDERS",
                }


def stage_ecr(store, snapshot, *, reviews_only=False, now=None):
    """Persist reviewed strategy inputs; no credentials or requests, no approval signing."""
    if snapshot.synthetic or store.namespace != "live":
        raise AuthorizationRequired("REAL_ECR_STAGE_REQUIRED")
    count = 0
    with store.transaction() as s:
        for c in snapshot.candidates:
            if reviews_only:
                from types import SimpleNamespace

                if now is None:
                    raise SchemaMismatch("REVIEW_STAGING_TIME_REQUIRED")

                def approved(review):
                    return bool(
                        review.approved
                        and review.reviewer
                        and review.approved_at
                        and review.expires_at
                        and review.rules.kind == "OFFICIAL"
                        and review.rules.available_at
                        <= review.approved_at
                        <= now
                        < review.expires_at
                    )

                evaluated = SimpleNamespace(
                    rules_reviewed=approved(c.review),
                    mapping_reviewed=bool(c.review.mappings)
                    and all(
                        approved(m)
                        and m.event == c.event
                        and m.payoff_definition == c.review.payoff_definition
                        and m.approved_sig_rules_hash == c.rules_current_hash
                        for m in c.review.mappings
                    ),
                    rules_unchanged=c.rules_current_hash == c.review.rules.sha256,
                )
            else:
                evaluated = c.candidate(snapshot.now, False)
            if c.tick != Decimal("0.005") or c.lot != 1 or not c.exchange_id.isdecimal():
                raise SchemaMismatch("SIG_ORDER_GRID_OR_ID_MISMATCH")
            key = snapshot.tournament_id + ":" + c.market_id
            market = s.get(Market, key)
            node_reference = None
            if c.review.approved:
                try:
                    node_reference = json.loads(c.review.rules.text, parse_float=Decimal)
                    OfficialSchema().response("/markets/{id}/nodes", "get", node_reference)
                    if node_reference["market_id"] != c.market_id:
                        raise SchemaMismatch("REVIEW_NODE_MARKET_MISMATCH")
                except (ValueError, SchemaMismatch):
                    raise ContractNotVerified("REVIEW_REQUIRES_OFFICIAL_SIG_NODE_JSON") from None
            if market is None:
                market = Market(
                    id=key,
                    tournament=snapshot.tournament_id,
                    api_id=c.market_id,
                    title=c.key,
                    category=c.event.office,
                    source="OFFICIAL_REVIEWED_INPUT",
                    raw={"reviewed_sig_nodes": json.loads(json.dumps(node_reference, default=str))},
                    rules_hash=c.rules_current_hash,
                )
                s.add(market)
                s.flush()
            else:
                market.raw = {
                    **market.raw,
                    "reviewed_sig_nodes": json.loads(json.dumps(node_reference, default=str)),
                }
            inst = s.get(Instrument, c.instrument_id)
            if inst is None:
                inst = Instrument(id=c.instrument_id)
                s.add(inst)
            inst.exchange_id, inst.market_id, inst.tournament = (
                c.exchange_id,
                key,
                snapshot.tournament_id,
            )
            inst.event_group, inst.outcome = c.event.event_key(False), c.side
            inst.rules_hash, inst.mapping_hash = c.rules_current_hash, c.review.mapping_hash
            inst.mapping_evidence = {
                "source": "OFFICIAL",
                "input_hash": content_hash(c.model_dump(mode="json")),
            }
            inst.tick, inst.step = c.tick, c.lot
            inst.fee_per_share = c.review.buy_fee_bound
            inst.reviewed = (
                evaluated.rules_reviewed
                and evaluated.mapping_reviewed
                and evaluated.rules_unchanged
            )
            inst.review_until = c.review.expires_at
            inst.status, inst.quality = "OPEN", "UNKNOWN"
            inst.book, inst.book_at = {}, None  # Staged prices are not a live refresh.
            count += 1
    return {
        "status": "STAGED",
        "instruments": count,
        "submitted": False,
        "credentials_loaded": False,
    }


def run_command(args, config):
    """Finite operator invocation; never launches a service or buys infrastructure."""
    from sigbot.domain import Intent
    from sigbot.execution import Executor
    from sigbot.operations import Controls
    from sigbot.readcheck import load_key
    from sigbot.reconciliation import Reconciler
    from sigbot.storage import Store
    from sigbot.strategies.ecr_sig_v1.inputs import Snapshot
    from sigbot.strategies.ecr_sig_v1.oms import entry_intent
    from sigbot.strategies.router import StrategyRouter

    store = Store(config.database_url, "live")
    exchange, executor = None, None
    clock = Clock()
    try:
        if args.command == "strategy" and args.action == "stage":
            return stage_ecr(store, Snapshot.model_validate_json(Path(args.input).read_text())), 0
        if not args.authorize_read:
            raise AuthorizationRequired("EXPLICIT_READ_AUTHORIZATION_REQUIRED")
        approval_path = getattr(args, "approval_file", None)
        approval = (
            Approval.model_validate_json(Path(approval_path).read_text()) if approval_path else None
        )
        exchange = SigExecutionExchange(store, config, clock, load_key(args.env_file), approval)
        controls = Controls(store, clock)
        if args.command == "account" and args.action == "bootstrap":
            snapshot = exchange.snapshot()
            controls.initialize_live(snapshot)
            return {
                "status": "CHECKPOINT_IMPORTED",
                "reconciliation": exchange.last_account_check,
                "real_orders": 0,
                "real_cancels": 0,
            }, 0
        cup = exchange.call(
            "tournament_identity", path_params={"slug": config.expected_tournament_slug}
        )
        if cup["id"] != config.expected_tournament_id:
            raise AuthorizationRequired("LIVE_IDENTITY_MISMATCH")
        # Verified current tournament dates; never the first/default context.
        starts, ends = utc_time(cup["startDate"]), utc_time(cup["endDate"])
        planner = StrategyRouter(store, config, clock, starts, ends)
        executor = Executor(store, config, clock, exchange, planner, config.expected_account_id)
        executor.start()
        # Recover all previously sent requests first, without submitting any again.
        with store.transaction() as s:
            pending = [
                o.id
                for o in s.scalars(select(Order).where(Order.account_id == exchange.account_id))
                if o.request_state in {"UNKNOWN", "SENDING"} or o.cancel_state == "CANCEL_UNKNOWN"
            ]
        for pending_id in pending:
            executor.resolve(pending_id)
        snap = exchange.snapshot()
        result = Reconciler(executor).run(snap)
        if args.command == "reconcile":
            return {"reconciliation": result, "requests": exchange.call_count}, 0 if result[
                "status"
            ] == "PASS" else 4
        if result["status"] != "PASS":
            return {
                "status": "BLOCKED",
                "reconciliation": result,
                "requests": exchange.call_count,
            }, 4
        if args.command == "orders" and args.action == "plan":
            from sigbot.domain import canonical

            controls.resume(exchange.account_id, "READ_ONLY_PLAN")
            intent = Intent.model_validate_json(Path(args.intent_file).read_text())
            exchange.refresh_book(intent.instrument_id)
            planned = {
                "intent": intent.model_dump(mode="json"),
                "plan": asdict(planner.preview(intent)),
                "bindings": bindings(config),
                "submitted": False,
                "write_attempts": exchange.call_count,
                "layer": "MOCK" if exchange.mock else "REAL_READ",
            }
            if args.output:
                private_write(Path(args.output), canonical(planned).encode())
            return planned, 0
        exchange.authorized()
        assert approval is not None
        controls.resume(exchange.account_id, approval.operator)
        if args.command == "orders" and args.action == "cancel":
            confirmed = executor.cancel(args.order_id)
            return {
                "cancel_confirmed": confirmed,
                "requests": exchange.call_count,
            }, 0 if confirmed else 6
        if args.command == "strategy":
            # Read after recovery: an upstream writer may keep this input current.
            snapshot = Snapshot.model_validate_json(Path(args.input).read_text())
            if snapshot.synthetic or (snapshot.account_id, snapshot.tournament_id) != (
                exchange.account_id,
                exchange.tournament_id,
            ):
                raise AuthorizationRequired("REAL_ECR_INPUT_SCOPE_MISMATCH")
            report = snapshot.plan()
            candidate_plans = {p["key"]: p for p in report["plans"]}
            from sigbot.strategies.ecr_sig_v1.oms import proposal_from_json

            intents = [
                entry_intent(snapshot, c, proposal_from_json(candidate_plans[c.key]))
                for c in snapshot.candidates
                if c.key in candidate_plans
            ]
        else:
            document = json.loads(Path(args.plan).read_text())
            intent = Intent.model_validate_json(json.dumps(document["intent"]))
            if document.get("bindings") != bindings(config):
                raise AuthorizationRequired("PLAN_BINDINGS_CHANGED")
            intents, report = [intent], {}
        outcomes = []
        for intent in intents:
            exchange.refresh_book(intent.instrument_id)
            if not exchange.mock:
                from datetime import datetime, timezone

                clock.now = datetime.now(timezone.utc)
            oid = planner.reserve(intent)
            obs = executor.dispatch(oid) if oid else None
            with store.transaction() as s:
                local_order = s.get(Order, oid) if oid else None
                order_state = local_order.state if local_order else None
            outcomes.append(
                {
                    "local_order": oid,
                    "observation": asdict(obs) if obs else None,
                    "order_state": order_state,
                }
            )
            if controls.status(exchange.account_id)["unknown_count"]:
                break
        status = (
            "NO_QUALIFIED_CANDIDATES"
            if not outcomes
            else (
                "OBSERVED"
                if all(o["observation"] for o in outcomes)
                else (
                    "REJECTED"
                    if all(o["order_state"] == "REJECTED" for o in outcomes)
                    else "UNKNOWN_OR_NO_ORDER"
                )
            )
        )
        return {
            "status": status,
            "outcomes": outcomes,
            "strategy": report,
            "requests": exchange.call_count,
            "layer": "MOCK" if exchange.mock else "LIVE_CANARY",
            "execution_mode": config.mode.value,
        }, 0 if status in {"OBSERVED", "NO_QUALIFIED_CANDIDATES"} else (
            3 if status == "REJECTED" else 6
        )
    except SigbotError as exc:
        if exchange:
            exc.execution_attempts = dict(exchange.call_count)
        raise
    except (ValueError, OSError):
        failure = SchemaMismatch("LIVE_INPUT_OR_RECEIPT_INVALID")
        if exchange:
            failure.execution_attempts = dict(exchange.call_count)
        raise failure from None
    except SQLAlchemyError:
        storage_failure = StorageUnavailable("LIVE_STORAGE_UNAVAILABLE")
        if exchange:
            storage_failure.execution_attempts = dict(exchange.call_count)
        raise storage_failure from None
    finally:
        if executor:
            executor.close()
        if exchange:
            exchange.close()
        store.engine.dispose()
