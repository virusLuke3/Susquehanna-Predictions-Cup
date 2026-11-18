# Susquehanna Predictions Cup

Strategy and order execution code for the 2026 U.S. Midterm Elections edition of the Susquehanna Predictions Cup. Entrants traded YES and NO contracts with 100,000 virtual SUSQies in starting capital. The contest allowed automated trading via its official API. [Official rules](https://predictionscup.com/rules/)

## Strategies

- **ECR-SIG** compares equivalent election contracts across venues. It looks for a cheaper executable price on SIG, sizes entries using fees and order-book depth, and exits on SIG as the gap closes or the reference weakens. [ECR code](src/sigbot/strategies/ecr_sig_v1/)
- **PRC-SIG** compares comparable polls for the same race and checks for a confirming SIG price move in the first 15 minutes after publication. It checks entry cost and exit liquidity before sizing an order. [PRC code](src/sigbot/strategies/prc_sig_v1/)
- **MM-SIG** quotes passive bids and offers on SIG. It adjusts quotes and sizes with inventory, limits sales to holdings, and checks the spread against execution costs. [MM code](src/sigbot/strategies/mm_sig_v1/)

The strategy package also contains the Grid, SSR, and semantic election implementations used by the shared execution layer. [Strategy code](src/sigbot/strategies/)

## Order execution

The order CLI exposes `orders plan`, `orders submit`, and `orders cancel`. The strategies use the shared order manager for capital reservations, fill processing, recovery, and reconciliation. Authorized API use requires local contracts and schemas, credentials, approval files, and an initialized database. [Order implementation](src/sigbot/live.py)

```bash
python -m pip install -e .
sigbot strategy list
sigbot orders --help
```

## Confirmed contest fills

The fill export contains **202 confirmed fills across 122 orders**. It comes from the complete account fill read retained on November 2, 2026 UTC, with buy and sell directions matched to order receipts. The fills occurred between October 3 and October 24, 2026 UTC. Prices are in SUSQies per share and quantities are positive share counts. [Fill data](data/fills.csv)
