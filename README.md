# Susquehanna Predictions Cup

We built a Python trading system for the **2026 U.S. Midterm Elections** edition of the Susquehanna Predictions Cup. The repository contains our strategy code, shared order executor, and confirmed contest fills.

## The competition

Susquehanna International Group ran an individual simulated trading contest from October 1 to November 4, 2026. Each participant started with 100,000 virtual SUSQies and traded YES or NO contracts on U.S. midterm election outcomes. Participants could automate trades through the official API. Final rankings use the balances after the markets settle. [Official rules](https://predictionscup.com/rules/) · [Platform guide](https://sig.thesuper.market/docs/tournaments-and-organizations)

## Our three main strategies

### ECR-SIG · Cross-venue repricing

ECR looks for an election contract that has repriced on an external venue while the equivalent SIG contract remains cheaper. The strategy matches the underlying race, outcome, and settlement rules before using a price as a reference.

1. **Find the gap.** The original signal looks for persistent repricing confirmed by two external venues, or by one venue and a verified public catalyst. The Polymarket reference variant compares the current external bid with the SIG ask directly.
2. **Price the trade at size.** ECR subtracts entry and exit fee bounds and a discount to the external reference. It walks the order books to check the expected entry price and how much could be sold back on SIG. Cash and exposure limits across races, states, and directional groups constrain the size.
3. **Manage the position on SIG.** An entry uses a fixed-share limit order. Exit rules check the executable SIG bid against the target and stop, and account for reference reversal, a missing reference, and the time spent in the position. [ECR planner](src/sigbot/strategies/ecr_sig_v1/core.py) · [Polymarket reference variant](src/sigbot/strategies/ecr_sig_v1/reference.py)

For example, a reference bid of 0.56 and a SIG ask of 0.46 leave a gross gap of 0.10 SUSQies per share. The strategy still needs enough depth and a large enough gap after its cost adjustments. Both entry and exit orders go to SIG.

### PRC-SIG · Poll changes with price confirmation

PRC tests whether new polling information is followed by a sustained price move on SIG. The poll triggers an evaluation, and SIG prices determine which side qualifies.

1. **Measure the poll change.** Compare the new poll with the most recent comparable poll for the same race. The implementation checks data provenance, sample comparability, and the uncertainty of the change in the polling margin. It requires a move of at least three percentage points in magnitude and an absolute z-score of at least two.
2. **Wait for the price to confirm.** Compare the pre-release midpoint with readings at 5, 10, and 15 minutes. A qualifying side must show a modest initial rise, hold or extend that move, and still have a narrow spread at entry. The strategy evaluates the actual YES and NO books and requires exactly one side to pass.
3. **Size the trade and define the exit.** The order size depends on entry and exit depth, round-trip costs, and portfolio limits. Limits also prevent concentration in the same state, report, or survey sample. The position logic checks a cost-adjusted profit target, stop, holding deadline, and invalidation of the poll signal. [PRC planner](src/sigbot/strategies/prc_sig_v1/core.py) · [Poll and price gates](src/sigbot/strategies/prc_sig_v1/poll_math.py)

### MM-SIG · Inventory-aware market making

MM supplies liquidity with passive limit orders. Its goal is to buy and sell at different prices while controlling the inventory left by one-sided fills.

1. **Build quotes from the SIG order book.** Set the reference bid and ask using liquidity from other participants. Round quotes to the instrument’s tick and keep buy orders below the current ask and sell orders above the current bid.
2. **Adjust to inventory and costs.** As inventory builds, the baseline policy shifts quotes downward, reduces the buy size, and limits sales to shares held. The spread must cover fees and an exit-cost buffer. The quoting logic also checks book freshness and recent trade activity. Older inventory can put the baseline policy into sell-only quoting.
3. **Track the outcome of each quote.** The order manager tracks fills, cancellations, and outstanding inventory. The measurement code records the price move after a fill to assess adverse selection and uses confirmed trades to account for closed positions. [MM quote engine](src/sigbot/strategies/mm_sig_v1/core.py) · [Fill measurements](src/sigbot/strategies/mm_sig_v1/measurements.py)

## Additional strategy code

- **Grid** uses a rolling median and price dispersion to estimate a trading range. Its order runner checks the current book, tracks held lots, and manages target and risk exits. [Grid profile](src/sigbot/strategies/grid_sig.py) · [Grid order runner](src/sigbot/strategies/grid_v2_runtime.py)
- **SSR** evaluates pairs of reviewed NO contracts on the same race. It compares the cost of both legs at the intended size with the reviewed payoff bound and has recovery logic for a position where only one leg fills. [SSR runner](src/sigbot/strategies/ssr_runtime.py)
- **Semantic election research** groups contracts by race and evaluates bundles against reviewed settlement conditions, possible payout scenarios, and executable order-book costs. [Semantic scanner](src/sigbot/strategies/semantic_sig_v1/core.py)

## From signal to execution

Strategies produce order intents with a limit price and quantity. The shared order manager checks the account budget and reserves capital before submission. It records order state, applies confirmed fills to the ledger, and reconciles positions against the venue. A request with an uncertain result keeps its recorded state for recovery. [Order CLI](src/sigbot/cli.py) · [Executor](src/sigbot/execution.py) · [Shared gateway](src/sigbot/shared_execution.py)

To inspect the strategy register and order commands locally, run these from the repository root.

```bash
python -m pip install -e .
sigbot doctor --mode offline
sigbot strategy list
sigbot orders --help
```

The default mode is offline. Authorized API sessions require a configured account, locally supplied contracts and schemas, approval files, and an initialized database.

## Confirmed contest fills

The export contains **202 fills across 122 orders**, including 119 buy fills and 83 sell fills. The retained account snapshot is from November 2, 2026 UTC, and the fills span October 3 to October 24 UTC. Each row contains the fill and order IDs, timestamp, market and exchange IDs, outcome, action, executed price in SUSQies, and a positive share count. [Fill data](data/fills.csv)
