from datetime import timedelta

from sqlalchemy import select
from sqlalchemy.orm import Session

from sigbot.config import Config
from sigbot.domain import Clock, utc_time
from sigbot.errors import RateLimited, SchemaMismatch
from sigbot.storage import Account, RateAttempt, Store


class Scheduler:
    def __init__(self, store: Store, config: Config, clock: Clock):
        self.store, self.config, self.clock = store, config, clock

    def charge_in_transaction(
        self, s: Session, account: Account, budget: str, weight: int = 1
    ) -> None:
        if (
            budget not in {"READ", "NEW", "CANCEL"}
            or not isinstance(weight, int)
            or isinstance(weight, bool)
            or weight <= 0
        ):
            raise SchemaMismatch("RATE_CLASS_UNVERIFIED")
        if account.budget_blocked_until and utc_time(account.budget_blocked_until) > self.clock.now:
            raise RateLimited("ACCOUNT_BACKOFF")
        rows = list(
            s.scalars(
                select(RateAttempt).where(
                    RateAttempt.account_id == account.id,
                    RateAttempt.at > self.clock.now - timedelta(seconds=60),
                )
            )
        )
        read = sum(r.weight for r in rows if r.budget == "READ")
        write = sum(r.weight for r in rows if r.budget != "READ")
        new = sum(r.weight for r in rows if r.budget == "NEW")
        if (
            (budget == "READ" and read + weight > self.config.read_budget)
            or (budget != "READ" and write + weight > self.config.write_budget)
            or (budget == "NEW" and new + weight > self.config.new_order_budget)
        ):
            raise RateLimited("RATE_LIMITED_LOCAL")
        s.add(RateAttempt(account_id=account.id, budget=budget, weight=weight, at=self.clock.now))

    def charge(self, account_id: str, budget: str, weight: int = 1) -> None:
        with self.store.transaction() as s:
            a = self.store.lock_account(s, account_id)
            self.charge_in_transaction(s, a, budget, weight)

    def backoff(self, account_id: str, seconds: int) -> None:
        if seconds < 0:
            raise SchemaMismatch("INVALID_RETRY_AFTER")
        with self.store.transaction() as s:
            a = self.store.lock_account(s, account_id)
            until = self.clock.now + timedelta(seconds=seconds)
            a.budget_blocked_until = (
                max(utc_time(a.budget_blocked_until), until) if a.budget_blocked_until else until
            )
