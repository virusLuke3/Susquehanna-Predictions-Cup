"""Recover from proven pre-HTTP failure or verified expiry with no venue effect."""

import json
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

from sqlalchemy import select

from sigbot.authorization import Approval
from sigbot.domain import content_hash, utc_time
from sigbot.errors import Conflict, ContractNotVerified
from sigbot.live import code_hash
from sigbot.storage import (
    AccountReadCheckpoint,
    Audit,
    IntentRow,
    LiveWire,
    Order,
    RequestAttempt,
    Store,
)


def expired_no_order_evidence(
    baseline, result, data, identity, coverage, anchor, payload, sent_at, observed_at
):
    """Prove no venue effect using a pre-send baseline and causally complete history.

    An expired POST's original HTTP outcome stays unknown. This proves its
    remaining economic exposure is zero, without attributing an order by amount.
    """
    from sigbot.accountcheck import financial_state

    expiry = utc_time(payload["expirationDate"])
    version = anchor.get("asOf") or {}
    if (
        baseline["status"] != "PASS"
        or result["status"] != "PASS"
        or not result.get("pagination_complete")
        or not result.get("financial_state_unchanged")
        or not baseline["data"]["result"].get("pagination_complete")
        or not baseline["data"]["result"].get("financial_state_unchanged")
        or not utc_time(baseline["observed_at"]) < utc_time(sent_at) < expiry
        or observed_at < expiry + timedelta(seconds=90)
        or anchor.get("exchangeId") != payload["exchangeId"]
        or type(version.get("sequence")) is not int
        or version["sequence"] < 0
        or not version.get("at")
        or not expiry < utc_time(version["at"]) <= observed_at
        or data["cup"]["id"] != payload["tournamentId"]
        or identity["balance"] != Decimal(result["projected_cash"])
        or not baseline["digest"] == content_hash(baseline["data"])
        or content_hash(baseline["data"]["financial_state"]) != content_hash(financial_state(data))
    ):
        return None
    for name in ("open_orders", "fills"):
        pages = coverage.get(name, [])
        if not pages or any(
            not page
            or page.get("complete") is not True
            or type(page.get("projectedThroughSequence")) is not int
            or page["projectedThroughSequence"] < version["sequence"]
            for page in pages
        ):
            return None
    return {
        "kind": "EXPIRED_SUBMIT_NO_VENUE_EFFECT",
        "original_http_outcome": "UNKNOWN",
        "remote_order_id_inferred": False,
        "baseline_checkpoint": baseline["checkpoint"],
        "baseline_observed_at": baseline["observed_at"],
        "anchor": anchor,
        "coverage": coverage,
        "observed_at": observed_at.isoformat(),
        "expiry": payload["expirationDate"],
        "sent_at": str(sent_at),
        "financial_state_digest": content_hash(financial_state(data)),
        "projected_cash": result["projected_cash"],
        "positions": result["positions"],
        "order_count": len(data["orders"]),
        "fill_count": len(data["fills"]),
        "account_id": identity["id"],
        "tournament_id": data["cup"]["id"],
    }


def prove_expired_no_order(exchange, order_id):
    """Use the existing official reader/budget, only under the account/order claim."""
    from sigbot.accountcheck import checkpoint

    exchange.clock.sync()
    with exchange.store.transaction() as s:
        order, wire = s.get(Order, order_id), s.get(LiveWire, order_id)
        row = s.get(IntentRow, order.intent_id)
        if (
            order.remote_id
            or wire is None
            or wire.receipt
            or order.filled
            or order.cancel_state
            or order.request_state not in {"SENDING", "UNKNOWN"}
            or row.payload.get("strategy_id") != "mm_sig"
            or row.payload.get("strategy_version") != "1.3"
            or wire.payload.get("idempotencyKey") != order_id
            or not wire.payload.get("expirationDate")
            or exchange.clock.now < utc_time(wire.payload["expirationDate"]) + timedelta(seconds=90)
            or wire.digest != content_hash(wire.payload)
        ):
            return None
        transmitted = s.scalar(
            select(Audit)
            .where(
                Audit.account_id == order.account_id,
                Audit.kind == "HTTP_TRANSMISSION_STARTED",
                Audit.data["order"].as_string() == order_id,
            )
            .order_by(Audit.at)
            .limit(1)
        )
        last = s.scalar(
            select(Audit)
            .where(
                Audit.account_id == order.account_id,
                Audit.kind == "EXPIRED_NO_ORDER_CHECK_STARTED",
                Audit.data["order"].as_string() == order_id,
            )
            .order_by(Audit.at.desc())
            .limit(1)
        )
        if not transmitted or last and (exchange.clock.now - last.at).total_seconds() < 60:
            return None
        payload, digest, aid, sent_at = (
            dict(wire.payload),
            wire.digest,
            order.account_id,
            transmitted.at,
        )
        if (aid, payload["tournamentId"]) != (exchange.account_id, exchange.tournament_id):
            raise Conflict("EXPIRED_PROOF_SCOPE_MISMATCH")
    spec = exchange.schema.spec
    orders = spec["paths"]["/orders"]["get"]["responses"]["200"]["content"]["application/json"][
        "schema"
    ]["properties"]["coverage"]
    fills = spec["paths"]["/tournaments/{slug}/portfolio/fills"]["get"]["responses"]["200"][
        "content"
    ]["application/json"]["schema"]["properties"]["coverage"]
    expiration = spec["components"]["schemas"]["OrderInput"]["properties"]["expirationDate"]
    if (
        "complete order history without window truncation"
        not in orders["properties"]["complete"]["description"]
        or "complete portfolio fill history" not in fills["properties"]["complete"]["description"]
        or "after which the limit order expires" not in expiration["description"]
    ):
        raise ContractNotVerified("EXPIRED_NO_ORDER_HISTORY_CONTRACT_REQUIRED")
    readonly = Store(exchange.store.engine.url, "readonly")
    try:
        with readonly.transaction() as s:
            before = s.scalar(
                select(AccountReadCheckpoint)
                .where(
                    AccountReadCheckpoint.account_id == aid,
                    AccountReadCheckpoint.tournament_id == payload["tournamentId"],
                    AccountReadCheckpoint.status == "PASS",
                    AccountReadCheckpoint.observed_at < sent_at,
                )
                .order_by(AccountReadCheckpoint.observed_at.desc())
                .limit(1)
            )
            if before is None:
                return None
            baseline = {
                "status": before.status,
                "observed_at": before.observed_at.isoformat(),
                "checkpoint": before.checkpoint,
                "digest": before.digest,
                "data": before.data,
            }
    finally:
        readonly.engine.dispose()
    with exchange.store.transaction() as s:
        exchange.store.audit(
            s,
            aid,
            "EXPIRED_NO_ORDER_CHECK_STARTED",
            {"order": order_id, "wire_digest": digest},
            exchange.clock.now,
        )
    anchor = exchange.call(
        "orderbook",
        path_params={"id": payload["exchangeId"]},
        query={"tournamentId": payload["tournamentId"], "depth": 200},
    )
    result, data = checkpoint(exchange)
    identity = exchange.call("account_identity", query={"tournamentId": payload["tournamentId"]})
    if identity["id"] != aid:
        raise Conflict("EXPIRED_PROOF_IDENTITY_CHANGED")
    proof = expired_no_order_evidence(
        baseline,
        result,
        data,
        identity,
        exchange.history_coverage,
        {k: anchor.get(k) for k in ["exchangeId", "marketId", "asOf"]},
        payload,
        sent_at,
        exchange.clock.sync(),
    )
    if proof:
        proof.update(
            order_id=order_id, wire_digest=digest, layer="MOCK" if exchange.mock else "REAL_READ"
        )
    return proof


# Reviewed immutable deployment: READ_SESSION_LIMIT_REACHED can occur before
# writer.request, or after a successful POST whose remote ID/receipt is committed.
LEGACY_PRE_HTTP_VERSIONS = {"2a0018426aba39fc99588db889485899fabeeb01c8e0fedf2495bc5514ccd193"}


def legacy_not_sent_proof(store, order_id, approval_file, code_root):
    raw = json.loads(Path(approval_file).read_text())
    raw.pop("eligibility_confirmed_by_operator", None)
    approval = Approval.model_validate(raw)
    if (
        approval.code_hash not in LEGACY_PRE_HTTP_VERSIONS
        or code_hash(Path(code_root)) != approval.code_hash
    ):
        raise Conflict("LEGACY_TRANSPORT_VERSION_NOT_REVIEWED")
    with store.transaction() as s:
        order, wire = s.get(Order, order_id), s.get(LiveWire, order_id)
        if (
            order is None
            or wire is None
            or order.account_id != approval.account_id
            or order.request_state not in {"UNKNOWN", "SENDING"}
            or order.remote_id
            or order.filled
            or order.cancel_state
            or wire.receipt
            or wire.approval_id != approval.approval_id
            or wire.approval_digest != content_hash(approval.model_dump(mode="json"))
        ):
            raise Conflict("LEGACY_NOT_SENT_PROOF_SCOPE_OR_RECEIPT_CONFLICT")
        attempts = list(
            s.scalars(select(RequestAttempt).where(RequestAttempt.order_id == order_id))
        )
        if len(attempts) != 1 or attempts[0].action != "SUBMIT":
            raise Conflict("LEGACY_MULTIPLE_OR_NON_SUBMIT_ATTEMPTS")
        events = list(s.scalars(select(Audit).where(Audit.account_id == order.account_id)))
        if any(
            e.data.get("order") == order_id
            and e.kind in {"LIVE_RECEIPT", "HTTP_TRANSMISSION_STARTED"}
            for e in events
        ):
            raise Conflict("LEGACY_TRANSMISSION_EVIDENCE_PRESENT")
        failures = [
            e
            for e in events
            if e.kind == "SHARED_COMMAND_BLOCKED"
            and e.data.get("order_id") == order_id
            and e.data.get("code") == "READ_SESSION_LIMIT_REACHED"
            and 0 <= (utc_time(e.at) - utc_time(attempts[0].sent_at)).total_seconds() <= 5
        ]
        if len(failures) != 1:
            raise Conflict("LEGACY_PRE_HTTP_FAILURE_AUDIT_NOT_UNIQUE")
        return {
            "phase": "REVIEWED_LEGACY_HTTP_PREPARATION",
            "local_code": "READ_SESSION_LIMIT_REACHED",
            "failure_audit_id": failures[0].id,
            "transport_code_hash": approval.code_hash,
            "wire_digest": wire.digest,
            "attempt_id": attempts[0].id,
        }
