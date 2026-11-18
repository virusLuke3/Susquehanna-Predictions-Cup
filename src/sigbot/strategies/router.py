"""Registered policies share one account allocator transaction and one Executor."""

from sigbot.domain import Intent
from sigbot.errors import RiskRejected
from sigbot.risk import Planner
from sigbot.storage import IntentRow


class StrategyRouter(Planner):
    def market_data_time(self, inst, intent):
        return self.policy(intent).market_data_time(inst, intent)

    def can_coexist(self, intent, existing):
        return self.policy(intent).can_coexist(intent, existing)

    def check_reduce_pending_buy(self, intent, pending_buy):
        return self.policy(intent).check_reduce_pending_buy(intent, pending_buy)

    def policy(self, intent):
        cls: type[Planner]
        identity = (intent.strategy_id, intent.strategy_version)
        if identity == ("manual_target", "1.0"):
            cls = Planner
        elif identity == ("prc_sig", "1.0"):
            from sigbot.strategies.prc_sig_v1.oms import PrcPlanner

            cls = PrcPlanner
        elif identity == ("ecr_sig", "1.0"):
            from sigbot.strategies.ecr_sig_v1.oms import EcrPlanner

            cls = EcrPlanner
        elif identity == ("ecr_sig", "1.1"):
            from sigbot.strategies.ecr_sig_v1.reference import ReferencePlanner

            cls = ReferencePlanner
        elif identity == ("mm_sig", "1.0"):
            from sigbot.strategies.mm_sig_v1.oms import MmPlanner

            cls = MmPlanner
        elif identity in {("mm_sig", "1.1"), ("mm_sig", "1.2"), ("mm_sig", "1.3")}:
            from sigbot.strategies.mm_sig_v1.live.execution import LivePlanner

            cls = LivePlanner
        elif identity == ("semantic_sig", "1.0"):
            from sigbot.strategies.semantic_sig_v1.live import SemanticPlanner

            cls = SemanticPlanner
        elif identity == ("ssr_sig", "1.0"):
            from sigbot.strategies.ssr import SsrPlanner

            cls = SsrPlanner
        elif identity == ("grid_sig", "2.0"):
            from sigbot.strategies.grid_v2 import RiskGridPlanner

            cls = RiskGridPlanner
        elif identity == ("grid_sig", "1.0"):
            from sigbot.strategies.grid_sig import GridPlanner

            cls = GridPlanner
        else:
            raise RiskRejected("STRATEGY_NOT_REGISTERED")
        return cls(self.store, self.config, self.clock, self.starts_at, self.ends_at)

    def calculate(self, s, account, intent):
        return self.policy(intent).calculate(s, account, intent)

    def check_liquidity(self, inst, side, price, quantity, intent):
        self.policy(intent).check_liquidity(inst, side, price, quantity, intent)

    def on_reserved(self, s, account, intent, plan, order_id):
        self.policy(intent).on_reserved(s, account, intent, plan, order_id)

    def check_dispatch(self, s, account, intent, order):
        self.policy(intent).check_dispatch(s, account, intent, order)

    def on_observed(self, s, account, order, observation):
        intent = Intent.model_validate(s.get(IntentRow, order.intent_id).payload)
        try:
            policy = self.policy(intent)
        except RiskRejected as exc:
            if exc.code != "STRATEGY_NOT_REGISTERED":
                raise
            # Another versioned worker may own a newer strategy. The common
            # ledger has already ingested this receipt; its owner projects the
            # strategy lifecycle. This never grants allocation/write authority.
            return
        policy.on_observed(s, account, order, observation)

    def on_unsent_expired(self, s, account, intent, order):
        self.policy(intent).on_unsent_expired(s, account, intent, order)

    def on_unsent_canceled(self, s, account, intent, order):
        self.policy(intent).on_unsent_canceled(s, account, intent, order)
