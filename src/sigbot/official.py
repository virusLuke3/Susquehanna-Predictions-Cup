from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from urllib.parse import quote

import httpx
from jsonschema import Draft202012Validator
from referencing import Registry, Resource

from sigbot.contracts import Contract
from sigbot.errors import (
    AuthorizationRequired,
    ContractNotVerified,
    HistoryReadError,
    SchemaMismatch,
)
from sigbot.transport import ReadTransport

SPEC_URI = "urn:sigbot:official-openapi"


def pointer(value: str) -> str:
    return value.replace("~", "~0").replace("/", "~1")


class OfficialSchema:
    def __init__(self, path: str = "evidence/api/openapi-2026-10-03.json"):
        self.path = path
        self.spec = json.loads(Path(path).read_text())
        document = {"$schema": "https://json-schema.org/draft/2020-12/schema", **self.spec}
        self.registry = Registry().with_resource(SPEC_URI, Resource.from_contents(document))

    def validate(self, path: str, value) -> None:
        validator = Draft202012Validator({"$ref": SPEC_URI + path}, registry=self.registry)
        if next(validator.iter_errors(value), None) is not None:
            raise SchemaMismatch("OFFICIAL_SCHEMA_MISMATCH")

    def response(self, path: str, method: str, value) -> None:
        self.validate(
            "#/paths/"
            + pointer(path)
            + "/"
            + method
            + "/responses/200/content/application~1json/schema",
            value,
        )

    def response_error_paths(self, path: str, method: str, value):
        """Structural error locations only; never expose response values or secrets."""
        ref = (
            "#/paths/"
            + pointer(path)
            + "/"
            + method
            + "/responses/200/content/application~1json/schema"
        )
        validator = Draft202012Validator({"$ref": SPEC_URI + ref}, registry=self.registry)
        return [
            (tuple(error.absolute_path), error.validator) for error in validator.iter_errors(value)
        ]


@dataclass(frozen=True)
class ReportedOrder:
    order_id: str
    exchange_id: str
    tournament_id: str | None
    side: str
    action: str
    reported_quantity: Decimal
    open: bool
    interpretation: str


@dataclass(frozen=True)
class ReportedFill:
    trade_id: str
    order_id: str | None
    exchange_id: str
    side: str
    signed_quantity: Decimal
    price: Decimal | None
    fee: None = None  # Official fill schema has no fee: never fill in zero.


class DocumentedReads:
    """Evidence-backed reads; reported records remain separate from synthetic OMS records."""

    def __init__(
        self,
        config,
        contract: Contract,
        charge,
        pause,
        transport=None,
        observe=None,
        documented_read_session: bool = False,
    ):
        self.config, self.contract = config, contract
        self.schema = OfficialSchema(
            contract.data.get("schema_path", "evidence/api/openapi-2026-10-03.json")
        )
        self.mock = isinstance(transport, httpx.MockTransport)
        self.documented_read_session = documented_read_session
        self.transport = ReadTransport(
            config,
            charge,
            pause,
            transport=transport,
            contract=contract,
            observe=observe,
            documented_read_session=documented_read_session,
        )

    def call(
        self,
        name: str,
        *,
        path_params: dict | None = None,
        query: dict | None = None,
        auth_value: str,
    ) -> dict:
        if (
            not isinstance(auth_value, str)
            or not auth_value.startswith("Bearer ")
            or not auth_value[7:].strip()
        ):
            raise SchemaMismatch("OFFICIAL_AUTH_FORMAT_REQUIRED")
        cap = self.contract.data["capabilities"].get(name)
        if (
            cap is None
            or cap.get("status") not in {"DOCUMENTED", "VERIFIED"}
            or cap.get("http_method") != "GET"
        ):
            raise ContractNotVerified("DOCUMENTED_READ_UNAVAILABLE:" + name)
        evidence = cap.get("evidence", [])
        if not evidence or any(
            e.get("source") != "OFFICIAL"
            or e.get("evidence_path") != self.schema.path
            or e.get("sha256") != hashlib.sha256(Path(self.schema.path).read_bytes()).hexdigest()
            for e in evidence
        ):
            raise ContractNotVerified("OFFICIAL_DOCUMENT_HASH_MISMATCH")
        if not self.mock:
            from sigbot.domain import Mode

            if (
                self.documented_read_session
                and self.config.mode in {Mode.READ_ONLY, Mode.LIVE_CANARY, Mode.LIVE_LIMITED}
                and self.config.read_session_authorized
            ):
                self.contract.require_documented_read(name)
            else:
                self.contract.require(name)
        path = cap["openapi_path"]
        path_params, query = path_params or {}, query or {}
        endpoint = self.schema.spec["paths"][path]["get"]
        supplied = {"path": path_params, "query": query}
        params = endpoint.get("parameters", [])
        if (
            any(p["name"] == "tournamentId" for p in params)
            and query.get("tournamentId") != self.config.expected_tournament_id
        ):
            raise AuthorizationRequired("EXPLICIT_TOURNAMENT_SCOPE_REQUIRED")
        if (
            any(p["name"] == "slug" for p in params)
            and path_params.get("slug") != self.config.expected_tournament_slug
        ):
            raise AuthorizationRequired("TOURNAMENT_SLUG_SCOPE_MISMATCH")
        for location in supplied:
            if set(supplied[location]) - {p["name"] for p in params if p["in"] == location}:
                raise SchemaMismatch("UNKNOWN_REQUEST_PARAMETER")
        resolved = path
        for index, param in enumerate(params):
            location, field = param["in"], param["name"]
            if field not in supplied[location]:
                if param.get("required"):
                    raise SchemaMismatch("MISSING_REQUEST_PARAMETER")
                continue
            value = supplied[location][field]
            self.schema.validate(
                "#/paths/" + pointer(path) + "/get/parameters/" + str(index) + "/schema", value
            )
            if isinstance(value, str) and "SIM-" in value:
                raise AuthorizationRequired("SYNTHETIC_REAL_TRANSPORT_FORBIDDEN")
            if location == "path":
                resolved = resolved.replace("{" + field + "}", quote(value, safe=""))
        refs = [
            self.config.expected_account_id or "UNCONFIGURED",
            self.config.expected_tournament_id or "UNCONFIGURED",
        ]
        if "UNCONFIGURED" in refs:
            if name not in {"account_identity", "tournament_list"}:
                raise AuthorizationRequired("EXPLICIT_READ_IDENTITY_REQUIRED")
            refs = ["READ-BOOTSTRAP"]
        try:
            raw = self.transport.request(
                {"name": name, **cap},
                "Authorization",
                auth_value,
                refs,
                query,
                path_params=path_params,
            )
        except HistoryReadError as exc:
            response = endpoint.get("responses", {}).get("410")
            global_cursor_contract = (
                exc.code == "CURSOR_EXPIRED"
                and self.contract.data.get("order_history_policy")
                == "CANCELLED_UNFILLED_ARCHIVED_AFTER_48H"
                and query.get("cursor")
            )
            if response is None and not global_cursor_contract:
                raise ContractNotVerified("OFFICIAL_HISTORY_ERROR_NOT_REVIEWED") from None
            self.schema.validate(
                "#/paths/" + pointer(path) + "/get/responses/410/content/application~1json/schema"
                if response is not None
                else "#/components/schemas/OrderError",
                exc.payload,
            )
            raise
        try:
            data = json.loads(
                raw,
                parse_float=Decimal,
                parse_constant=lambda _: (_ for _ in ()).throw(ValueError("nonfinite JSON")),
            )
        except (ValueError, UnicodeError):
            raise SchemaMismatch("INVALID_OFFICIAL_JSON") from None
        self.schema.response(path, "get", data)
        return data

    def identity(self, auth_value: str) -> dict:
        parameters = self.schema.spec["paths"]["/account"]["get"].get("parameters", [])
        query = (
            {"tournamentId": self.config.expected_tournament_id}
            if self.config.expected_tournament_id
            and any(p["name"] == "tournamentId" and p["in"] == "query" for p in parameters)
            else {}
        )
        data = self.call("account_identity", query=query, auth_value=auth_value)
        if data["id"] != self.config.expected_account_id:
            raise AuthorizationRequired("IDENTITY_MISMATCH")
        return {"id": data["id"], "default_context_balance": data["balance"]}

    def iter_pages(
        self,
        name: str,
        *,
        path_params: dict | None = None,
        query: dict | None = None,
        auth_value: str,
        allow_archived_orders: bool = False,
    ):
        query = dict(query or {})
        seen: set[str] = set()
        cursors: set[str] = set()
        pages: list[dict] = []
        restarted = False
        for _ in range(100):
            try:
                data = self.call(name, path_params=path_params, query=query, auth_value=auth_value)
            except HistoryReadError as exc:
                if exc.code != "CURSOR_EXPIRED" or not query.get("cursor") or restarted:
                    raise
                # No page escapes until this traversal completes. A restart
                # discards the old traversal, rather than duplicating its rows.
                query.pop("cursor")
                pages, seen, cursors, restarted = [], set(), set(), True
                continue
            for row in data["data"]:
                if "id" in row:
                    key = str(row["id"])
                    if key in seen:
                        raise SchemaMismatch("DUPLICATE_HISTORY_ID")
                    seen.add(key)
            pages.append(data)
            if data.get("coverage", {}).get("complete") is False and not (
                allow_archived_orders and self.archived_orders_allowed(name, query)
            ):
                raise SchemaMismatch("REMOTE_COVERAGE_PARTIAL")
            pg = data["pagination"]
            if not pg["hasMore"]:
                if pg["nextCursor"] is not None:
                    raise SchemaMismatch("PAGINATION_TERMINAL_CONFLICT")
                yield from pages
                return
            cursor = pg["nextCursor"]
            if not cursor or cursor in cursors or not data["data"]:
                raise SchemaMismatch("CURSOR_LOOP_OR_EMPTY_PAGE")
            cursors.add(cursor)
            query["cursor"] = cursor
        raise SchemaMismatch("LIVE_HISTORY_PAGE_BOUND_REACHED")

    def archived_orders_allowed(self, name, query):
        """Only the reviewed all-orders history can omit archived zero-fill cancels."""
        return (
            name == "open_orders"
            and query.get("status") == "all"
            and self.contract.data.get("order_history_policy")
            == "CANCELLED_UNFILLED_ARCHIVED_AFTER_48H"
        )

    def order(self, raw: dict) -> ReportedOrder:
        self.schema.validate("#/components/schemas/Order", raw)
        return ReportedOrder(
            str(raw["id"]),
            raw["exchangeId"],
            raw["tournamentId"],
            raw["side"],
            raw["action"],
            Decimal(raw["quantity"]),
            raw["open"],
            "EXECUTABLE_REMAINDER" if raw["open"] else "CLOSED_UNCLASSIFIED_REPORT_SIZE",
        )

    def fill(self, raw: dict) -> ReportedFill:
        self.schema.validate("#/components/schemas/Fill", raw)
        quantity = Decimal(raw["quantity"])
        if (
            (quantity > 0 and raw["side"] != "yes")
            or (quantity < 0 and raw["side"] != "no")
            or quantity == 0
        ):
            raise SchemaMismatch("OUTCOME_SIGNED_FILL_MISMATCH")
        return ReportedFill(
            str(raw["id"]),
            str(raw["orderId"]) if raw["orderId"] is not None else None,
            raw["exchangeId"],
            raw["side"],
            quantity,
            Decimal(raw["price"]) if raw["price"] is not None else None,
        )

    def close(self):
        self.transport.close()


def documented_limit_payload(
    exchange_id: str,
    tournament_id: str,
    side: str,
    action: str,
    quantity: Decimal,
    price: Decimal,
    idempotency_key: str,
) -> dict:
    """Offline plan payload only; exact Decimal JSON serialization is not a live submit path."""
    from sigbot.domain import check_grid

    check_grid(price, quantity, Decimal("0.005"), Decimal("1"))
    if not tournament_id or not idempotency_key or exchange_id.startswith("SIM-"):
        raise SchemaMismatch("DOCUMENTED_LIMIT_SCOPE_REQUIRED")
    payload = {
        "exchangeId": exchange_id,
        "tournamentId": tournament_id,
        "side": side,
        "action": action,
        "quantity": int(quantity),
        "price": price,
        "idempotencyKey": idempotency_key,
    }
    OfficialSchema().validate("#/components/schemas/SingleOrderInput", payload)
    return payload
