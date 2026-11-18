"""Credential-free producers. Only the account executor allocates these proposals."""

from datetime import timedelta

from sqlalchemy import select

from sigbot.domain import Intent, content_hash, utc_time
from sigbot.errors import AuthorizationRequired, Conflict
from sigbot.storage import ExecutionCommand, Instrument, IntentRow, Order


class CommandQueue:
    def __init__(self, store, account_id, strategy_id, strategy_version):
        self.store, self.account_id = store, account_id
        self.identity = strategy_id, strategy_version

    def publish(self, intent: Intent):
        if (intent.account_id, intent.strategy_id, intent.strategy_version) != (
            self.account_id,
            *self.identity,
        ) or intent.synthetic != (self.store.namespace in {"offline", "shadow"}):
            raise AuthorizationRequired("STRATEGY_QUEUE_SCOPE_MISMATCH")
        return self._put(
            "SUBMIT",
            intent.intent_id,
            intent.instrument_id,
            intent.model_dump(mode="json"),
            intent.created_at,
            intent.expires_at,
        )

    def publish_preallocated(self, session, intent: Intent, order_id):
        """Queue an OMS order inside the transaction that reserved a multi-leg basket."""
        if (intent.account_id, intent.strategy_id, intent.strategy_version) != (
            self.account_id,
            *self.identity,
        ) or intent.synthetic != (self.store.namespace in {"offline", "shadow"}):
            raise AuthorizationRequired("STRATEGY_QUEUE_SCOPE_MISMATCH")
        order = session.get(Order, order_id)
        row = session.get(IntentRow, intent.intent_id)
        if (
            order is None
            or row is None
            or row.digest != intent.digest
            or (order.account_id, order.instrument_id, order.intent_id)
            != (self.account_id, intent.instrument_id, intent.intent_id)
        ):
            raise AuthorizationRequired("STRATEGY_QUEUE_RESERVATION_BINDING_MISMATCH")
        key = ("SIM-COMMAND-" if intent.synthetic else "SIG-COMMAND-") + content_hash(
            [self.account_id, *self.identity, "SUBMIT", intent.intent_id]
        )
        old = session.get(ExecutionCommand, key)
        if old:
            if (
                old.digest != content_hash(intent.model_dump(mode="json"))
                or old.order_id != order_id
            ):
                raise Conflict("STRATEGY_QUEUE_CONTENT_CONFLICT")
            return key
        payload = intent.model_dump(mode="json")
        session.add(
            ExecutionCommand(
                id=key,
                account_id=self.account_id,
                strategy_id=self.identity[0],
                strategy_version=self.identity[1],
                action="SUBMIT",
                instrument_id=intent.instrument_id,
                order_id=order_id,
                payload=payload,
                digest=content_hash(payload),
                created_at=intent.created_at,
                expires_at=intent.expires_at,
                state="OMS_PENDING",
                result={
                    "order_id": order_id,
                    "allocated": True,
                    "allocation": "COMMON_ACCOUNT_TRANSACTION",
                },
            )
        )
        self.store.audit(
            session,
            self.account_id,
            "STRATEGY_PROPOSAL_QUEUED",
            {"command": key, "action": "SUBMIT", "order": order_id},
            intent.created_at,
        )
        return key

    def cancel(self, order_id, now, *, reason=None, safety=True):
        with self.store.transaction() as s:
            order = s.get(Order, order_id)
            row = s.get(IntentRow, order.intent_id) if order else None
            if (
                order is None
                or row is None
                or order.account_id != self.account_id
                or (row.payload["strategy_id"], row.payload["strategy_version"]) != self.identity
            ):
                raise AuthorizationRequired("STRATEGY_QUEUE_CANCEL_SCOPE_MISMATCH")
            instrument = order.instrument_id
            payload = {"order_id": order_id}
            if reason is not None:
                payload.update(
                    order_generation=order.intent_id,
                    venue_order_id=order.remote_id,
                    strategy_revision="1.3.1",
                    reason=reason,
                    risk_latched=safety,
                )
        return self._put("CANCEL", order_id, instrument, payload, now, now + timedelta(minutes=5))

    def _put(self, action, logical_id, instrument_id, payload, created, expires):
        key = (
            "SIM-COMMAND-" if self.account_id.startswith("SIM-") else "SIG-COMMAND-"
        ) + content_hash([self.account_id, *self.identity, action, logical_id])
        digest = content_hash(payload)
        with self.store.transaction() as s:
            account = self.store.lock_account(s, self.account_id)
            old = s.get(ExecutionCommand, key)
            if old:
                if action == "CANCEL" and payload.get("strategy_revision") == "1.3.1":
                    order = s.get(Order, logical_id)
                    if order.cancel_state or old.state == "OMS_CLOSED":
                        return key  # A sent/unknown cancel cannot be revoked or retargeted.
                    if old.payload.get("risk_latched", True) and old.payload.get("reason"):
                        from sigbot.strategies.mm_sig_v1.live.lifecycle import (
                            RECOVERABLE_DATA_CANCELS,
                        )

                        if not (
                            old.state == "SUPERSEDED_BEFORE_SEND"
                            and old.payload["reason"] in RECOVERABLE_DATA_CANCELS
                        ):
                            return key  # Ordinary targets cannot clear an unresolved risk latch.
                    old.payload, old.digest = payload, digest
                    self.store.audit(
                        s,
                        self.account_id,
                        "MM131_CANCEL_REVIEW",
                        {
                            "command": key,
                            "order": logical_id,
                            "reason": payload.get("reason", "UNSPECIFIED"),
                            "risk_latched": payload.get("risk_latched", False),
                        },
                        utc_time(created),
                    )
                    if old.state in {"SUPERSEDED_BEFORE_SEND", "EXPIRED_BEFORE_ALLOCATION"}:
                        old.created_at, old.expires_at = utc_time(created), utc_time(expires)
                        old.state, old.result = "WAITING_ALLOCATION", {}
                    return key
                if old.digest != digest:
                    raise Conflict("STRATEGY_QUEUE_CONTENT_CONFLICT")
                if (
                    action == "CANCEL"
                    and old.state == "EXPIRED_BEFORE_ALLOCATION"
                    and old.order_id is None
                ):
                    old.created_at, old.expires_at = utc_time(created), utc_time(expires)
                    old.state, old.result = "WAITING_ALLOCATION", {}
                return key
            inst = s.get(Instrument, instrument_id)
            if inst is None or inst.tournament != account.tournament:
                raise AuthorizationRequired("STRATEGY_QUEUE_INSTRUMENT_SCOPE_MISMATCH")
            if action == "SUBMIT":
                for previous in s.scalars(
                    select(ExecutionCommand).where(
                        ExecutionCommand.account_id == self.account_id,
                        ExecutionCommand.strategy_id == self.identity[0],
                        ExecutionCommand.strategy_version == self.identity[1],
                        ExecutionCommand.instrument_id == instrument_id,
                        ExecutionCommand.action == "SUBMIT",
                        ExecutionCommand.state == "WAITING_ALLOCATION",
                    )
                ):
                    if previous.payload["action"] == payload["action"]:
                        previous.state = "SUPERSEDED_BEFORE_ALLOCATION"
            s.add(
                ExecutionCommand(
                    id=key,
                    account_id=self.account_id,
                    strategy_id=self.identity[0],
                    strategy_version=self.identity[1],
                    action=action,
                    instrument_id=instrument_id,
                    payload=payload,
                    digest=digest,
                    created_at=utc_time(created),
                    expires_at=utc_time(expires),
                    state="WAITING_ALLOCATION",
                    result={},
                )
            )
            self.store.audit(
                s,
                account.id,
                "STRATEGY_PROPOSAL_QUEUED",
                {"command": key, "action": action},
                utc_time(created),
            )
        return key
