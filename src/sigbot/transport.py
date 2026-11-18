import math
import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Callable
from urllib.parse import quote, urlsplit

import httpx

from sigbot.config import Config
from sigbot.domain import Mode
from sigbot.errors import (
    AuthorizationRequired,
    HistoryReadError,
    RateLimited,
    SchemaMismatch,
    TransportUnknown,
)

OFFICIAL_ORIGIN = "https://sig.thesuper.market"


def validate_auth(header: str, value: str) -> None:
    if (
        not re.fullmatch(r"[A-Za-z0-9-]+", header)
        or not value
        or any(c in value for c in "\r\n\x00")
    ):
        raise SchemaMismatch("INVALID_AUTH_INPUT")
    if header.lower() in {"cookie", "host", "proxy-authorization"}:
        raise SchemaMismatch("FORBIDDEN_AUTH_HEADER")


def safe_log(request_id: str, code: str, status: int | None = None) -> dict:
    # Arbitrary exceptions/responses are never logged. Callers pass internal codes only.
    allowed = {"AUTHENTICATION_FAILED", "RATE_LIMITED", "TRANSPORT_UNKNOWN", "SCHEMA_MISMATCH"}
    return {
        "request_id": request_id,
        "code": code if code in allowed else "SCHEMA_MISMATCH",
        "status": status,
    }


class ReadTransport:
    """Guarded raw transport. No SIG field decoder or write implementation is claimed."""

    def __init__(
        self,
        config: Config,
        charge: Callable,
        pause: Callable,
        transport: httpx.BaseTransport | None = None,
        contract=None,
        observe=None,
        documented_read_session: bool = False,
    ):
        self.config, self.charge, self.pause = config, charge, pause
        self.contract = contract
        self.observe = observe
        self.documented_read_session = documented_read_session
        self.last_response: dict = {}
        self.mock_transport = isinstance(transport, httpx.MockTransport)
        self.client = httpx.Client(
            transport=transport,
            verify=True,
            follow_redirects=False,
            trust_env=False,
            timeout=httpx.Timeout(15, connect=5, pool=5),
        )

    def request(
        self,
        capability: dict,
        header: str,
        value: str,
        refs: list[str],
        payload: dict | None = None,
        path_params: dict | None = None,
    ) -> bytes:
        if not self.mock_transport:
            from sigbot.errors import ContractNotVerified

            if self.contract is None or not isinstance(capability.get("name"), str):
                raise ContractNotVerified("READ_TRANSPORT_CONTRACT_REQUIRED")
            verified = (
                self.contract.require_documented_read(capability["name"])
                if self.documented_read_session
                and self.config.mode in {Mode.READ_ONLY, Mode.LIVE_CANARY, Mode.LIVE_LIMITED}
                and self.config.business_network_enabled
                and self.config.read_session_authorized
                else self.contract.require(capability["name"])
            )
            if capability != {"name": capability["name"], **verified}:
                raise ContractNotVerified("READ_TRANSPORT_CAPABILITY_CHANGED")
        if self.config.mode == Mode.OFFLINE or not (
            self.config.business_network_enabled and self.config.read_session_authorized
        ):
            raise AuthorizationRequired("REAL_READ_NOT_AUTHORIZED")
        if capability["business_permission"] not in {"READ_MARKET", "READ_ACCOUNT", "AUTH_SESSION"}:
            raise AuthorizationRequired("LIVE_WRITES_DISABLED_IN_THIS_DELIVERY")
        if any(r.startswith("SIM-") for r in refs) or not refs:
            raise AuthorizationRequired("SYNTHETIC_REAL_TRANSPORT_FORBIDDEN")
        validate_auth(header, value)
        path = capability["path"]
        for key, field in (path_params or {}).items():
            if not isinstance(field, str) or not field or "{" + key + "}" not in path:
                raise SchemaMismatch("INVALID_OFFICIAL_PATH_PARAMETERS")
            path = path.replace("{" + key + "}", quote(field, safe=""))
        if "{" in path or "}" in path:
            raise SchemaMismatch("MISSING_OFFICIAL_PATH_PARAMETERS")
        if not isinstance(path, str) or not path.startswith("/api/v1/") or urlsplit(path).netloc:
            raise SchemaMismatch("INVALID_OFFICIAL_PATH")
        method = capability["http_method"]
        if method not in {"GET", "POST"}:
            raise SchemaMismatch("UNSUPPORTED_READ_METHOD")
        if method == "POST" and capability["business_permission"] != "AUTH_SESSION":
            raise SchemaMismatch("POST_READ_REQUIRES_DOCUMENTED_ENCODING")
        if self.config.mode == Mode.READ_ONLY and method != "GET":
            raise AuthorizationRequired("READ_ONLY_GET_REQUIRED")
        self.last_response = {}
        self.charge(capability["rate_budget_class"], capability["rate_weight"])
        try:
            response = self.client.request(
                method,
                OFFICIAL_ORIGIN + path,
                headers={header: value},
                params=payload if method == "GET" else None,
                json=payload if method == "POST" else None,
            )
        except httpx.HTTPError:
            raise TransportUnknown("TRANSPORT_UNKNOWN") from None
        # Never publish arbitrary server errors, headers, profiles, or exception strings.
        self.last_response = {"http_status": response.status_code}
        token = value.removeprefix("Bearer ").encode()
        echoed = token in response.content
        if self.observe is not None:
            self.observe(
                capability.get("name", ""),
                response.status_code,
                response.content.replace(token, b"[REDACTED]") if echoed else response.content,
            )
        if response.status_code == 429:
            retry = response.headers.get("Retry-After", "")
            seconds = 60
            if retry.isdecimal():
                seconds = max(1, int(retry))
            else:
                try:
                    seconds = max(
                        1,
                        math.ceil(
                            (
                                parsedate_to_datetime(retry) - datetime.now(timezone.utc)
                            ).total_seconds()
                        ),
                    )
                except (TypeError, ValueError, OverflowError):
                    pass
            self.last_response["retry_after_seconds"] = seconds
        if response.status_code in (401, 403):
            self.pause("AUTHENTICATION_FAILED")
            raise AuthorizationRequired("AUTHENTICATION_FAILED")
        if response.status_code == 429:
            self.pause("RATE_LIMITED")
            raise RateLimited("RATE_LIMITED")
        if response.is_redirect:
            raise SchemaMismatch("REDIRECT_FORBIDDEN")
        if echoed:
            raise SchemaMismatch("CREDENTIAL_ECHO_FORBIDDEN")
        if response.status_code == 410:
            try:
                payload = response.json()
            except (ValueError, UnicodeError):
                payload = None
            error = payload.get("error") if isinstance(payload, dict) else None
            code = error.get("code") if isinstance(error, dict) else None
            if code in {"ORDER_HISTORY_ARCHIVED", "CURSOR_EXPIRED"}:
                raise HistoryReadError(code, payload)
        if response.status_code >= 400:
            raise TransportUnknown("READ_RESPONSE_UNAVAILABLE")
        return response.content

    def close(self) -> None:
        self.client.close()


def __getattr__(name):
    # Lazy import: official.py uses ReadTransport while the execution bridge uses
    # official.py's validators. Keep the historical adapter import usable.
    if name == "SigExchange":
        from sigbot.live import SigExecutionExchange

        return SigExecutionExchange
    raise AttributeError(name)
