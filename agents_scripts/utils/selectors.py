```python
# selectors.py
"""
CSS Selectors for newhedge.io Bitcoin Dashboard
"""

SELECTORS = {
    # Live Price and Header Metrics
    "live_price": "#graphName span:last-child",
    "price_change_pct": "#realTimeChangeContainer",
    "24h_high": ".stat-value.ath_price:first-of-type",
    "24h_low": ".stat-value:not(.ath_price):first-of-type",
    "24h_vol_btc": ".stat-value:nth-child(3)",
    "24h_vol_usd": ".stat-value:has(span:contains('B')):first-of-type",
    "ath_usd": ".ath_price:last-of-type",
    "market_cap": "#marketcap",
    "open_interest": ".stat-value:has(span:contains('36.57B'))",
    "btc_dominance": "#btc_dominance",

    # Blockchain Metrics
    "block_height": ".block-height",
    "last_block_time": ".last-block",
    "block_speed": "#block_speed",
    "blocks_24h": "#blocks_per_day",
    "outputs_24h": "#outputs_per_day",

    # Fear & Greed
    "fear_greed_value": ".fear-greed-gauge-value",
    "fear_greed_label": ".fear-greed-gauge-text",

    # Mining
    "hashrate": "#hashrate",
    "hashprice": "#HashPrice",

    # Fees
    "fees_per_tx_sats": "#feesPerTransactionBtc",
    "fees_per_tx_usd": "#feesPerTransactionUsd",
    "fees_24h_btc": "#feesPerDayBtc",
    "fees_24h_usd": "#feesPerDayUsd",

    # Supply
    "circulating_supply": "#supply",
    "percent_issued": "#percentIssued",
    "issuance_remaining": "#IssuanceRemaining",
    "total_mined_blocks": "#height",
    "daily_issuance": "#daily_issuance",

    # Corporate Treasuries
    "public_companies": ".dashboard-subcol p:nth-child(1):last-child",  # Adjust based on structure
    "public_holdings_btc": ".dashboard-primary-text:first-of-type",
    "private_companies": ".dashboard-subcol p:nth-child(3):last-child",
    "private_holdings_btc": ".dashboard-primary-text:nth-child(2)",
    "total_corp_holdings_btc": ".dashboard-primary-text:last-child",
    "corp_supply_pct": ".dashboard-subcol:last-child p:last-child",

    # Government Treasuries
    "gov_count": ".dashboard-subcol:first-child p:last-child",
    "gov_btc": "#btcGovernments",
    "gov_usd": "#usdGovernments",
    "gov_supply_pct": ".dashboard-subcol:last-child p:last-child",

    # Transactions
    "tx_per_sec": "#transactions_per_second",
    "tx_per_block": "#transactions_per_block",
    "tx_per_day": "#transactions_per_day",
    "tx_month": ".dashboard-subcol:nth-child(4) p:last-child",
    "total_tx": "#total_transactions",

    # UTXOs
    "utxos_profit": ".dashboard-subcol:first-child p:last-child",
    "utxos_loss": ".dashboard-subcol:nth-child(2) p:last-child",
    "utxos_profit_pct": ".dashboard-subcol:last-child p:last-child",

    # Profitable Days
    "total_days": ".dashboard-subcol:first-child p:last-child",
    "profitable_days": ".dashboard-subcol:nth-child(2) p:last-child",
    "unprofitable_days": ".dashboard-subcol:nth-child(3) p:last-child",
    "profitable_pct": ".dashboard-subcol:last-child p:last-child",

    # Macro Liquidity
    "global_m2": ".dashboard-subcol:first-child p:last-child",
    "global_m2_growth": ".dashboard-subcol:nth-child(2) p:last-child",
    "global_m2_yoy": ".dashboard-subcol:nth-child(3) p:last-child",
    "global_m2_10w_lead": ".dashboard-subcol:nth-child(4) p:last-child",
    "us_m2": ".dashboard-subcol:nth-child(5) p:last-child",
    "fed_funds_rate": ".dashboard-subcol:last-child p:last-child",

    # ATH
    "ath_price": "#AthPriceMain",
    "drawdown_since_ath": "#price_drawdown_since_ath",
    "days_since_ath": ".drawdown-days-since-ath",
    "ath_date": "#AthPrice",

    # Trading Volumes
    "daily_btc_vol": "#DailyBtcVolume",
    "monthly_btc_vol": "#MonthlyBtcVolume",
    "binance_dominance": ".dashboard-subcol:nth-child(3) p:last-child",
    "btc_pairs_dominance": ".dashboard-subcol:nth-child(4) p:last-child",
    "us_crypto_vol": "#UsCryptoVolume",
    "offshore_crypto_vol": "#OffshoreCryptoVolume",

    # Price Performance
    "daily_perf": "#daily_price_performance",
    "weekly_perf": "#weekly_price_performance",
    "monthly_perf": "#monthly_price_performance",
    "quarterly_perf": "#quarterly_price_performance",

    # Gold Comparison
    "gold_price": "#GoldPrice",
    "gold_mcap": "#GoldMarketCap",
    "btc_vs_gold_mcap": ".dashboard-subcol:nth-child(3) p:last-child",
    "gold_correlation": ".dashboard-subcol:last-child p:last-child",
    "gold_supply": ".dashboard-subcol:nth-child(5) p:last-child",

    # Realized Prices
    "realized_price": "#RealizedPrice",
    "realized_mcap": "#RealizedMarketcap",
    "sth_realized": "#SthRealizedPrice",
    "lth_realized": "#LthRealizedPrice",

    # Address Balances
    "new_addresses": ".dashboard-subcol:first-child p:last-child",

    # On-chain Supply
    "lth_supply": ".dashboard-subcol:first-child p:last-child",
    "sth_supply": ".dashboard-subcol:nth-child(2) p:last-child",
    "supply_profit_pct": ".dashboard-subcol:nth-child(3) p:last-child",
    "total_profit_supply": ".dashboard-subcol:nth-child(4) p:last-child",
    "total_loss_supply": ".dashboard-subcol:last-child p:last-child",

    # Halving
    "halving_date": "#projected_halving_date",
    "halving_block": ".halving-at-block-selector",
    "blocks_remaining": "#blockRemaining",
    "btc_until_halving": "#btcUntilHalving",
    "epoch_pct": "#currentEpoch",

    # On-chain Indicators (from box)
    "cdd": ".dashboard-subcol:first-child p:last-child",
    "mvrv_z": ".dashboard-subcol:nth-child(2) p:last-child",
    "nvt": ".dashboard-subcol:nth-child(3) p:last-child",
    "rhodl": ".dashboard-subcol:nth-child(4) p:last-child",
    "reserve_risk": ".dashboard-subcol:nth-child(5) p:last-child",
    "vdd_multiple": ".dashboard-subcol:nth-child(6) p:last-child",
    "net_rpl": ".dashboard-subcol:nth-child(7) p:last-child",
    "nupl": ".dashboard-subcol:nth-child(8) p:last-child",
    "asol": ".dashboard-subcol:nth-child(9) p:last-child",
    "msol": ".dashboard-subcol:last-child p:last-child",
}

TABLE_SELECTORS = {
    "address_distribution": ".btc-address-table tbody tr",
    "polymarket_predictions": "#polymarket-table tbody tr",
    "mining_pool_dominance": ".mining-pool-dominance-pie [data-z-index='1'] text tspan",  # From SVG labels
    "node_distribution": ".nodes-box table tbody tr",  # Assume structure
    "node_versions": ".node-versions-box .dashboard-subcol",
    "futures_oi": ".derivatives-box .dashboard-subcol p:last-child",  # List
    "spot_etf": ".spot-etf-box .dashboard-subcol",
    "futures_etf": ".futures-etf-box .dashboard-subcol",
    "public_companies_top10": ".public-companies-box .dashboard-subcol",
    "private_companies_top10": ".private-companies-box .dashboard-subcol",
    "mining_stocks": ".mining-stocks-marketcaps-box .dashboard-subcol",
    "etf_holdings": ".etf-holdings-box .dashboard-subcol",
}
```
