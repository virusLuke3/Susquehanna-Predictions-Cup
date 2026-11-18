from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterator

from sqlalchemy import (
    JSON,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
    create_engine,
    event,
    select,
)
from sqlalchemy.engine import URL, make_url
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker
from sqlalchemy.types import TypeDecorator

from sigbot.domain import UTC, ZERO, utc_time
from sigbot.errors import AuthorizationRequired, Conflict


class ExactDecimal(TypeDecorator):
    impl = Numeric(60, 30)
    cache_ok = True

    def load_dialect_impl(self, dialect):
        # SQLite's numeric driver converts via binary float. Mock storage uses decimal text.
        return dialect.type_descriptor(String() if dialect.name == "sqlite" else Numeric(60, 30))

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        if not isinstance(value, Decimal) or not value.is_finite():
            raise ValueError("finite Decimal required")
        return str(value) if dialect.name == "sqlite" else value

    def process_result_value(self, value, dialect):
        return Decimal(value) if value is not None else None


AMOUNT = ExactDecimal()


class UTCDateTime(TypeDecorator):
    impl = DateTime(timezone=True)
    cache_ok = True

    def process_bind_param(self, value, dialect):
        return utc_time(value) if value is not None else None

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        # SQLite is a mock store and loses tz metadata; PostgreSQL retains timestamptz.
        return value.replace(tzinfo=UTC) if value.tzinfo is None else utc_time(value)


class Base(DeclarativeBase):
    pass


class Account(Base):
    __tablename__ = "accounts"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    namespace: Mapped[str] = mapped_column(String)
    tournament: Mapped[str] = mapped_column(String)
    cash: Mapped[Decimal] = mapped_column(AMOUNT)
    checkpoint: Mapped[str] = mapped_column(String)
    reconciled_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    lifecycle: Mapped[str] = mapped_column(String, default="RECOVERING")
    reason: Mapped[str] = mapped_column(String, default="STARTUP_REVIEW_REQUIRED")
    budget_blocked_until: Mapped[datetime | None] = mapped_column(UTCDateTime())
    __table_args__ = (CheckConstraint("CAST(cash AS NUMERIC) >= 0"),)


class Instrument(Base):
    __tablename__ = "instruments"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    exchange_id: Mapped[str | None] = mapped_column(String)
    mapping_evidence: Mapped[dict] = mapped_column(JSON, default=dict)
    market_id: Mapped[str] = mapped_column(ForeignKey("markets.id"))
    tournament: Mapped[str] = mapped_column(String)
    event_group: Mapped[str] = mapped_column(String)
    outcome: Mapped[str] = mapped_column(String)
    rules_hash: Mapped[str] = mapped_column(String)
    mapping_hash: Mapped[str] = mapped_column(String)
    tick: Mapped[Decimal] = mapped_column(AMOUNT)
    step: Mapped[Decimal] = mapped_column(AMOUNT)
    fee_per_share: Mapped[Decimal | None] = mapped_column(AMOUNT)
    reviewed: Mapped[bool] = mapped_column(Boolean, default=False)
    review_until: Mapped[datetime | None] = mapped_column(UTCDateTime())
    status: Mapped[str] = mapped_column(String, default="UNKNOWN")
    quality: Mapped[str] = mapped_column(String, default="UNKNOWN")
    book: Mapped[dict] = mapped_column(JSON, default=dict)
    book_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    __table_args__ = (UniqueConstraint("tournament", "exchange_id", "outcome"),)


class Market(Base):
    __tablename__ = "markets"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    tournament: Mapped[str] = mapped_column(String)
    page_id: Mapped[str | None] = mapped_column(String)
    api_id: Mapped[str | None] = mapped_column(String)
    title: Mapped[str] = mapped_column(String)
    category: Mapped[str] = mapped_column(String)
    source: Mapped[str] = mapped_column(String)
    rules_hash: Mapped[str | None] = mapped_column(String)
    raw: Mapped[dict] = mapped_column(JSON)
    __table_args__ = (UniqueConstraint("tournament", "api_id"),)


class RuleVersion(Base):
    __tablename__ = "market_versions"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    market_id: Mapped[str] = mapped_column(ForeignKey("markets.id"))
    rules_hash: Mapped[str] = mapped_column(String)
    text: Mapped[str] = mapped_column(String)
    evidence: Mapped[str] = mapped_column(String)
    observed_at: Mapped[datetime] = mapped_column(UTCDateTime())
    __table_args__ = (UniqueConstraint("market_id", "rules_hash"),)


class Review(Base):
    __tablename__ = "market_reviews"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    instrument_id: Mapped[str] = mapped_column(ForeignKey("instruments.id"))
    operator: Mapped[str] = mapped_column(String)
    rules_hash: Mapped[str] = mapped_column(String)
    mapping_hash: Mapped[str] = mapped_column(String)
    expires_at: Mapped[datetime] = mapped_column(UTCDateTime())
    revoked_reason: Mapped[str | None] = mapped_column(String)


class CatalogRun(Base):
    __tablename__ = "catalog_runs"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    tournament: Mapped[str] = mapped_column(String)
    status: Mapped[str] = mapped_column(String)
    evidence: Mapped[dict] = mapped_column(JSON)


class EventGroup(Base):
    __tablename__ = "event_groups"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    fields: Mapped[dict] = mapped_column(JSON)
    evidence: Mapped[str] = mapped_column(String)


class Relation(Base):
    __tablename__ = "relations"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    kind: Mapped[str] = mapped_column(String)
    evidence: Mapped[dict] = mapped_column(JSON)
    verified: Mapped[bool] = mapped_column(Boolean, default=False)


class MarketEventLink(Base):
    __tablename__ = "market_event_links"
    market_id: Mapped[str] = mapped_column(ForeignKey("markets.id"), primary_key=True)
    event_id: Mapped[str] = mapped_column(ForeignKey("event_groups.id"), primary_key=True)
    evidence: Mapped[dict] = mapped_column(JSON)
    verified: Mapped[bool] = mapped_column(Boolean, default=False)


class Position(Base):
    __tablename__ = "positions"
    account_id: Mapped[str] = mapped_column(ForeignKey("accounts.id"), primary_key=True)
    instrument_id: Mapped[str] = mapped_column(ForeignKey("instruments.id"), primary_key=True)
    quantity: Mapped[Decimal] = mapped_column(AMOUNT, default=ZERO)
    cost: Mapped[Decimal | None] = mapped_column(AMOUNT)
    __table_args__ = (
        CheckConstraint("CAST(quantity AS NUMERIC) >= 0"),
        CheckConstraint("cost IS NULL OR CAST(cost AS NUMERIC) >= 0"),
    )


class IntentRow(Base):
    __tablename__ = "order_intents"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    account_id: Mapped[str] = mapped_column(ForeignKey("accounts.id"))
    instrument_id: Mapped[str] = mapped_column(ForeignKey("instruments.id"))
    digest: Mapped[str] = mapped_column(String)
    payload: Mapped[dict] = mapped_column(JSON)
    active: Mapped[bool] = mapped_column(Boolean, default=True)


class ExecutionCommand(Base):
    """Strategy proposals awaiting allocation; accepted orders live in the OMS."""

    __tablename__ = "execution_commands"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    account_id: Mapped[str] = mapped_column(ForeignKey("accounts.id"), index=True)
    strategy_id: Mapped[str] = mapped_column(String)
    strategy_version: Mapped[str] = mapped_column(String)
    action: Mapped[str] = mapped_column(String)
    instrument_id: Mapped[str] = mapped_column(ForeignKey("instruments.id"))
    payload: Mapped[dict] = mapped_column(JSON)
    digest: Mapped[str] = mapped_column(String)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime())
    expires_at: Mapped[datetime] = mapped_column(UTCDateTime())
    state: Mapped[str] = mapped_column(String, default="WAITING_ALLOCATION", index=True)
    order_id: Mapped[str | None] = mapped_column(ForeignKey("orders.id"))
    result: Mapped[dict] = mapped_column(JSON, default=dict)


class StrategyEvent(Base):
    """Strategy lifecycle/exit fee reserve; economic positions remain in the shared ledger."""

    __tablename__ = "strategy_events"
    account_id: Mapped[str] = mapped_column(ForeignKey("accounts.id"), primary_key=True)
    strategy_id: Mapped[str] = mapped_column(String, primary_key=True)
    strategy_version: Mapped[str] = mapped_column(String, primary_key=True)
    event_key: Mapped[str] = mapped_column(String, primary_key=True)
    instrument_id: Mapped[str] = mapped_column(ForeignKey("instruments.id"))
    state: Mapped[str] = mapped_column(String)
    state_group: Mapped[str] = mapped_column(String)
    shock_group: Mapped[str] = mapped_column(String)
    direction_bucket: Mapped[str] = mapped_column(String)
    future_fee_reserve: Mapped[Decimal] = mapped_column(AMOUNT, default=ZERO)
    holding_started_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    last_exit_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    data: Mapped[dict] = mapped_column(JSON)
    __table_args__ = (CheckConstraint("CAST(future_fee_reserve AS NUMERIC) >= 0"),)


class PositionLot(Base):
    __tablename__ = "position_lots"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    account_id: Mapped[str] = mapped_column(ForeignKey("accounts.id"))
    instrument_id: Mapped[str] = mapped_column(ForeignKey("instruments.id"))
    economic_id: Mapped[str] = mapped_column(String)
    quantity: Mapped[Decimal] = mapped_column(AMOUNT)
    cost: Mapped[Decimal | None] = mapped_column(AMOUNT)
    __table_args__ = (UniqueConstraint("account_id", "instrument_id", "economic_id"),)


class Order(Base):
    __tablename__ = "orders"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    intent_id: Mapped[str] = mapped_column(ForeignKey("order_intents.id"), unique=True)
    account_id: Mapped[str] = mapped_column(ForeignKey("accounts.id"))
    instrument_id: Mapped[str] = mapped_column(ForeignKey("instruments.id"))
    remote_id: Mapped[str | None] = mapped_column(String)
    side: Mapped[str] = mapped_column(String)
    quantity: Mapped[Decimal] = mapped_column(AMOUNT)
    price: Mapped[Decimal] = mapped_column(AMOUNT)
    fee_bound: Mapped[Decimal] = mapped_column(AMOUNT)
    filled: Mapped[Decimal] = mapped_column(AMOUNT, default=ZERO)
    observed_filled: Mapped[Decimal] = mapped_column(AMOUNT, default=ZERO)
    state: Mapped[str] = mapped_column(String, default="UNOBSERVED")
    request_state: Mapped[str] = mapped_column(String, default="SEND_PENDING")
    cancel_state: Mapped[str | None] = mapped_column(String)
    revision: Mapped[int] = mapped_column(Integer, default=-1)
    reserve_cash: Mapped[Decimal] = mapped_column(AMOUNT, default=ZERO)
    reserve_quantity: Mapped[Decimal] = mapped_column(AMOUNT, default=ZERO)
    __table_args__ = (
        UniqueConstraint("account_id", "remote_id"),
        CheckConstraint(
            "CAST(filled AS NUMERIC) >= 0 AND CAST(filled AS NUMERIC) <= CAST(quantity AS NUMERIC)"
        ),
        CheckConstraint(
            "CAST(reserve_cash AS NUMERIC) >= 0 AND CAST(reserve_quantity AS NUMERIC) >= 0"
        ),
    )


class Outbox(Base):
    __tablename__ = "outbox"
    order_id: Mapped[str] = mapped_column(ForeignKey("orders.id"), primary_key=True)
    state: Mapped[str] = mapped_column(String, default="PENDING")


class LiveWire(Base):
    """Frozen official request and durable receipt, independent of the ledger projection."""

    __tablename__ = "live_wire"
    order_id: Mapped[str] = mapped_column(ForeignKey("orders.id"), primary_key=True)
    approval_id: Mapped[str] = mapped_column(String, index=True)
    approval_digest: Mapped[str] = mapped_column(String)
    payload: Mapped[dict] = mapped_column(JSON)
    digest: Mapped[str] = mapped_column(String)
    receipt: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    observation_digest: Mapped[str | None] = mapped_column(String)
    projection_revision: Mapped[int] = mapped_column(Integer, default=0)


class RequestAttempt(Base):
    __tablename__ = "request_attempts"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    order_id: Mapped[str] = mapped_column(ForeignKey("orders.id"))
    action: Mapped[str] = mapped_column(String)
    state: Mapped[str] = mapped_column(String)
    sent_at: Mapped[datetime] = mapped_column(UTCDateTime())


class Inbox(Base):
    __tablename__ = "inbox"
    account_id: Mapped[str] = mapped_column(ForeignKey("accounts.id"), primary_key=True)
    event_id: Mapped[str] = mapped_column(String, primary_key=True)
    digest: Mapped[str] = mapped_column(String)
    payload: Mapped[dict] = mapped_column(JSON)


class PollReportRow(Base):
    __tablename__ = "poll_reports"
    namespace: Mapped[str] = mapped_column(String, primary_key=True)
    release_id: Mapped[str] = mapped_column(String, primary_key=True)
    digest: Mapped[str] = mapped_column(String)
    first_public_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    received_at: Mapped[datetime] = mapped_column(UTCDateTime())
    available_at: Mapped[datetime] = mapped_column(UTCDateTime())
    data: Mapped[dict] = mapped_column(JSON)


class PollObservationRow(Base):
    __tablename__ = "poll_observations"
    namespace: Mapped[str] = mapped_column(String, primary_key=True)
    release_id: Mapped[str] = mapped_column(String, primary_key=True)
    question_id: Mapped[str] = mapped_column(String, primary_key=True)
    event_key: Mapped[str] = mapped_column(String, index=True)
    series_key: Mapped[str] = mapped_column(String)
    data: Mapped[dict] = mapped_column(JSON)


class StrategyEvaluation(Base):
    """Permanent release/event attempt latch; a confirmed exit never resets it."""

    __tablename__ = "strategy_evaluations"
    account_id: Mapped[str] = mapped_column(ForeignKey("accounts.id"), primary_key=True)
    strategy_id: Mapped[str] = mapped_column(String, primary_key=True)
    strategy_version: Mapped[str] = mapped_column(String, primary_key=True)
    release_id: Mapped[str] = mapped_column(String, primary_key=True)
    event_key: Mapped[str] = mapped_column(String, primary_key=True)
    evaluated_at: Mapped[datetime] = mapped_column(UTCDateTime())
    state: Mapped[str] = mapped_column(String)
    reason: Mapped[str | None] = mapped_column(String)
    order_id: Mapped[str | None] = mapped_column(ForeignKey("orders.id"))
    data: Mapped[dict] = mapped_column(JSON)


class StrategyDayRisk(Base):
    __tablename__ = "strategy_day_risk"
    account_id: Mapped[str] = mapped_column(ForeignKey("accounts.id"), primary_key=True)
    strategy_id: Mapped[str] = mapped_column(String, primary_key=True)
    strategy_version: Mapped[str] = mapped_column(String, primary_key=True)
    utc_day: Mapped[str] = mapped_column(String, primary_key=True)
    committed: Mapped[Decimal] = mapped_column(AMOUNT)
    __table_args__ = (CheckConstraint("CAST(committed AS NUMERIC) >= 0"),)


class MmRiskState(Base):
    __tablename__ = "mm_risk_state"
    account_id: Mapped[str] = mapped_column(ForeignKey("accounts.id"), primary_key=True)
    updated_at: Mapped[datetime] = mapped_column(UTCDateTime())
    data: Mapped[dict] = mapped_column(JSON)


class MmRiskEpisode(Base):
    __tablename__ = "mm_risk_episodes"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    account_id: Mapped[str] = mapped_column(ForeignKey("accounts.id"))
    instrument_id: Mapped[str] = mapped_column(ForeignKey("instruments.id"))
    event_key: Mapped[str] = mapped_column(String)
    started_at: Mapped[datetime] = mapped_column(UTCDateTime())
    closed_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    data: Mapped[dict] = mapped_column(JSON)
    __table_args__ = (Index("ix_mm_risk_episode_open", "account_id", "instrument_id", "closed_at"),)


class MmRiskOrder(Base):
    __tablename__ = "mm_risk_orders"
    order_id: Mapped[str] = mapped_column(ForeignKey("orders.id"), primary_key=True)
    episode_id: Mapped[str] = mapped_column(ForeignKey("mm_risk_episodes.id"))
    data: Mapped[dict] = mapped_column(JSON)


class EconomicEvent(Base):
    __tablename__ = "economic_events"
    account_id: Mapped[str] = mapped_column(ForeignKey("accounts.id"), primary_key=True)
    id: Mapped[str] = mapped_column(String, primary_key=True)
    kind: Mapped[str] = mapped_column(String)
    digest: Mapped[str] = mapped_column(String)
    payload: Mapped[dict] = mapped_column(JSON)


class FillRow(Base):
    __tablename__ = "fills"
    account_id: Mapped[str] = mapped_column(ForeignKey("accounts.id"), primary_key=True)
    id: Mapped[str] = mapped_column(String, primary_key=True)
    order_id: Mapped[str] = mapped_column(ForeignKey("orders.id"))
    payload: Mapped[dict] = mapped_column(JSON)


class Journal(Base):
    __tablename__ = "journal"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    account_id: Mapped[str] = mapped_column(ForeignKey("accounts.id"))
    economic_id: Mapped[str] = mapped_column(String)
    asset: Mapped[str] = mapped_column(String)
    bucket: Mapped[str] = mapped_column(String)
    amount: Mapped[Decimal] = mapped_column(AMOUNT)


class Audit(Base):
    __tablename__ = "audit_events"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    account_id: Mapped[str] = mapped_column(String)
    kind: Mapped[str] = mapped_column(String)
    data: Mapped[dict] = mapped_column(JSON)
    at: Mapped[datetime] = mapped_column(UTCDateTime())


Index("ix_audit_account_kind_at", Audit.account_id, Audit.kind, Audit.at)
Index(
    "ix_audit_account_kind_instrument_at",
    Audit.account_id,
    Audit.kind,
    Audit.data["instrument_id"].as_string(),
    Audit.at,
)


class RateAttempt(Base):
    __tablename__ = "rate_attempts"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    account_id: Mapped[str] = mapped_column(ForeignKey("accounts.id"))
    budget: Mapped[str] = mapped_column(String)
    weight: Mapped[int] = mapped_column(Integer)
    at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


class AccountReadCheckpoint(Base):
    """Immutable verified read projection, separate from executable OMS accounts."""

    __tablename__ = "account_read_checkpoints"
    namespace: Mapped[str] = mapped_column(String, primary_key=True)
    account_id: Mapped[str] = mapped_column(String, primary_key=True)
    tournament_id: Mapped[str] = mapped_column(String, primary_key=True)
    checkpoint: Mapped[str] = mapped_column(String, primary_key=True)
    status: Mapped[str] = mapped_column(String)
    observed_at: Mapped[datetime] = mapped_column(UTCDateTime())
    data: Mapped[dict] = mapped_column(JSON)
    digest: Mapped[str] = mapped_column(String)


class Store:
    def __init__(self, url: str | URL, namespace: str = "offline"):
        self.namespace = namespace
        parsed = make_url(url)
        if (
            namespace == "offline"
            and parsed.get_backend_name() == "postgresql"
            and parsed.host not in {"127.0.0.1", "localhost", "::1"}
        ):
            raise AuthorizationRequired("OFFLINE_EXTERNAL_DATABASE_FORBIDDEN")
        if parsed.get_backend_name() == "sqlite":
            if namespace != "offline":
                raise AuthorizationRequired("POSTGRES_REQUIRED_FOR_NON_OFFLINE")
            if parsed.database and parsed.database != ":memory:":
                Path(parsed.database).parent.mkdir(parents=True, exist_ok=True)
        self.engine = create_engine(url, pool_pre_ping=True)
        if self.engine.dialect.name == "sqlite":

            @event.listens_for(self.engine, "connect")
            def foreign_keys(conn: Any, _: Any) -> None:
                conn.execute("PRAGMA foreign_keys=ON")

        self.sessions = sessionmaker(self.engine, expire_on_commit=False)

    def create_for_mock(self) -> None:
        if self.engine.dialect.name != "sqlite":
            raise Conflict("USE_ALEMBIC_FOR_POSTGRES")
        Base.metadata.create_all(self.engine)

    @contextmanager
    def transaction(self) -> Iterator[Session]:
        with self.sessions.begin() as session:
            yield session

    def lock_account(self, session: Session, account_id: str) -> Account:
        account = session.scalar(select(Account).where(Account.id == account_id).with_for_update())
        if account is None:
            raise Conflict("ACCOUNT_NOT_INITIALIZED")
        if account.namespace != self.namespace:
            raise AuthorizationRequired("DATABASE_NAMESPACE_MISMATCH")
        if (self.namespace in {"offline", "shadow"}) != account.id.startswith("SIM-"):
            raise AuthorizationRequired("SYNTHETIC_ACCOUNT_NAMESPACE_MISMATCH")
        return account

    def audit(self, session: Session, account_id: str, kind: str, data: dict, at: datetime) -> None:
        from time import monotonic

        from sigbot.storage_policy import MARKET_AUDITS, bounded_storage, prune_market_audits

        if bounded_storage() and kind in MARKET_AUDITS and monotonic() >= getattr(self, "_next_market_prune", 0):
            prune_market_audits(session, at)
            self._next_market_prune = monotonic() + 300
        session.add(Audit(account_id=account_id, kind=kind, data=data, at=at))
