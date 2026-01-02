# selectors.py
"""
CSS Selectors for newhedge.io Bitcoin Dashboard
Updated based on actual HTML structure inspection
"""

SELECTORS = {
    # ===== TOP HEADER METRICS =====
    # These are found in the top banner with specific text patterns
    "24H_HIGH": {"type": "next_sibling", "text": "24H HIGH"},
    "24H_LOW": {"type": "next_sibling", "text": "24H LOW"},
    "24H_VOL_BTC": {"type": "next_sibling", "text": "24H VOL (BTC)"},
    "24H_VOL_USD": {"type": "next_sibling", "text": "24H VOL (USD)"},
    "ATH_USD": {"type": "next_sibling", "text": "ATH (USD)"},
    "MARKET_CAP_HEADER": {"type": "next_sibling", "text": "MARKET CAP"},
    "OPEN_INTEREST_HEADER": {"type": "next_sibling", "text": "OPEN INTEREST"},
    "BTC_DOMINANCE_HEADER": {"type": "next_sibling", "text": "BTC DOMINANCE"},
    
    # ===== LIVE PRICE SECTION =====
    "LIVE_PRICE": "#realTimePriceChange2",
    "LIVE_PRICE_CHANGE": "#realTimePercentChange2",
    "GRAPH_CHANGE": "#graphName",
    "REAL_TIME_CHANGE": "#realTimeChangeContainer",
    
    # ===== PRICE BOX =====
    "MARKET_CAP": "#marketcap",
    "BTC_DOMINANCE": "#btc_dominance",
    "SATS_PER_DOLLAR": "#satoshiPerDollar",
    
    # ===== BLOCKCHAIN BOX =====
    "BLOCK_HEIGHT": {"type": "next_sibling", "text": "Block Height"},
    "TIME_SINCE_LAST_BLOCK": {"type": "next_sibling", "text": "Time Since Last Block"},
    "BLOCK_SPEED": "#block_speed",
    "BLOCKS_24HRS": {"type": "next_sibling", "text": "Blocks (24hrs)"},
    "OUTPUTS_24HRS": {"type": "next_sibling", "text": "Outputs (24hrs)"},
    
    # ===== DIFFICULTY ADJUSTMENT =====
    "PREVIOUS_DIFFICULTY": {"type": "next_sibling", "text": "Previous Difficulty"},
    "PREVIOUS_DIFFICULTY_CHANGE": {"type": "next_sibling", "text": "Previous Difficulty Change"},
    "CURRENT_DIFFICULTY": {"type": "next_sibling", "text": "Current Difficulty"},
    "NEXT_DIFFICULTY_ESTIMATE": {"type": "next_sibling", "text": "Next Difficulty Estimate"},
    "NEXT_DIFFICULTY_CHANGE_PCT": {"type": "next_sibling", "text": "Next Difficulty Change %"},
    "NEXT_RETARGET": {"type": "next_sibling", "text": "Next Retarget"},
    "EPOCH": {"type": "next_sibling", "text": "Epoch"},
    
    # ===== FEAR & GREED INDEX =====
    "FEAR_GREED_INDEX": ".fear-greed-gauge-value",
    "FEAR_GREED_LABEL": ".fear-greed-gauge-text",
    
    # ===== MINING POOL DOMINANCE =====
    # This is in a pie chart, will need special handling
    
    # ===== BLOCK REWARD =====
    "REWARD_PER_BLOCK_BTC": {"type": "next_sibling", "text": "Reward per Block (BTC)"},
    "REWARD_PER_BLOCK_USD": "#usdReward",
    "REWARD_BTC_24HRS": {"type": "next_sibling", "text": "Reward (BTC) (24hrs)"},
    "REWARD_USD_24HRS": "#dailyUsdReward",
    
    # ===== MINERS =====
    "REVENUE_BTC_24HRS": {"type": "next_sibling", "text": "Revenue (BTC) (24hrs)"},
    "REVENUE_USD_24HRS": "#dailyRevenueUsd",
    "FEES_VS_REWARD_PCT": {"type": "next_sibling", "text": "Fees vs Reward %"},
    "CURRENT_MONTH_SUBSIDY_USD": "#MonthlySubsidy",
    "CURRENT_MONTH_FEES_USD": "#MonthlyFees",
    "CURRENT_MONTH_TOTAL_USD": "#MonthlyMinerRevenue",
    
    # ===== MINING =====
    "HASHRATE": {"type": "next_sibling", "text": "Hashrate"},
    "HASHPRICE": {"type": "next_sibling", "text": "Hashprice"},
    
    # ===== FEES =====
    "PER_TRANSACTION_SATS": {"type": "next_sibling", "text": "Per Transaction (sats)"},
    "PER_TRANSACTION_USD": "#feesPerTransactionUsd",
    "FEES_BTC_24HRS": {"type": "next_sibling", "text": "Fees (BTC) (24hrs)"},
    "FEES_USD_24HRS": "#feesPerDayUsd",
    
    # ===== SUPPLY =====
    "CIRCULATING_SUPPLY": {"type": "next_sibling", "text": "Circulating Supply"},
    "PERCENTAGE_ISSUED": {"type": "next_sibling", "text": "Percentage Issued"},
    "ISSUANCE_REMAINING": {"type": "next_sibling", "text": "Issuance Remaining"},
    "TOTAL_MINED_BLOCKS": {"type": "next_sibling", "text": "Total Mined Blocks"},
    "ISSUANCE_BTC_24HRS": {"type": "next_sibling", "text": "Issuance (BTC) (24hrs)"},
    
    # ===== CORPORATE TREASURIES =====
    "PUBLIC_COMPANIES_COUNT": {"type": "next_sibling", "text": "Public Companies"},
    "PUBLIC_HOLDINGS_BTC": {"type": "dashboard_primary", "context": "Public Holdings"},
    "PUBLIC_HOLDINGS_USD": {"type": "dashboard_secondary", "context": "Public Holdings"},
    "PRIVATE_COMPANIES_COUNT": {"type": "next_sibling", "text": "Private Companies"},
    "PRIVATE_HOLDINGS_BTC": {"type": "dashboard_primary", "context": "Private Holdings"},
    "PRIVATE_HOLDINGS_USD": {"type": "dashboard_secondary", "context": "Private Holdings"},
    "TOTAL_CORPORATE_BTC": {"type": "next_sibling", "text": "Total"},
    "CORPORATE_PCT_TOTAL_SUPPLY": {"type": "next_sibling", "text": "% of Total Supply"},
    
    # ===== GOVERNMENT TREASURIES =====
    "GOVERNMENTS_COUNT": {"type": "next_sibling", "text": "Governments"},
    "GOVERNMENT_BTC_TREASURIES": {"type": "next_sibling", "text": "BTC Held in Treasuries"},
    "GOVERNMENT_USD_TREASURIES": "#usdGovernments",
    "GOVERNMENT_PCT_TOTAL_SUPPLY": {"type": "css", "selector": "#government-treasuries-box .dashboard-subcol:nth-child(4) p:last-child"},
    
    # ===== UTXOs =====
    "UTXOS_IN_PROFIT": {"type": "next_sibling", "text": "Number of UTXOs in Profit"},
    "UTXOS_IN_LOSS": {"type": "next_sibling", "text": "Number of UTXOs in Loss"},
    "UTXOS_IN_PROFIT_PCT": {"type": "next_sibling", "text": "Percent UTXOs in Profit"},
    
    # ===== TRANSACTIONS =====
    "TRANSACTIONS_PER_SECOND": {"type": "next_sibling", "text": "Per Second"},
    "TRANSACTIONS_PER_BLOCK": {"type": "next_sibling", "text": "Per Block"},
    "TRANSACTIONS_PER_DAY": {"type": "next_sibling", "text": "Per Day"},
    "TRANSACTIONS_CURRENT_MONTH": {"type": "next_sibling", "text": "Current Month"},
    "TOTAL_TRANSACTIONS_ALL_TIME": {"type": "next_sibling", "text": "Total All Time"},
    
    # ===== PROFITABLE DAYS =====
    "TOTAL_DAYS": {"type": "next_sibling", "text": "Total Days"},
    "PROFITABLE_DAYS": {"type": "next_sibling", "text": "Profitable"},
    "UNPROFITABLE_DAYS": {"type": "next_sibling", "text": "Unprofitable"},
    "PERCENTAGE_PROFITABLE": {"type": "next_sibling", "text": "Percentage Profitable"},
    
    # ===== MACRO AND LIQUIDITY =====
    "GLOBAL_M2_SUPPLY": {"type": "next_sibling", "text": "Global M2 Supply"},
    "GLOBAL_M2_GROWTH": {"type": "next_sibling", "text": "Global M2 Supply Growth"},
    "GLOBAL_M2_YOY_GROWTH": {"type": "next_sibling", "text": "Global M2 YoY Growth"},
    "GLOBAL_M2_10WEEK_LEAD": {"type": "next_sibling", "text": "Global M2 10-Week Lead"},
    "US_M2_SUPPLY": {"type": "next_sibling", "text": "U.S M2 Supply"},
    "FEDERAL_FUNDS_RATE": {"type": "next_sibling", "text": "Federal Funds Rate"},
    
    # ===== ATH DETAILS =====
    "ATH_PRICE": {"type": "next_sibling", "text": "ATH Price"},
    "PRICE_DRAWDOWN_SINCE_ATH": {"type": "next_sibling", "text": "Price Drawdown Since ATH"},
    "DAYS_SINCE_ATH": {"type": "next_sibling", "text": "Days Since ATH"},
    "ATH_DATE": {"type": "next_sibling", "text": "ATH Date"},
    
    # ===== TRADING =====
    "DAILY_BTC_TRADING_VOL": {"type": "next_sibling", "text": "Daily BTC Trading Vol"},
    "MONTHLY_BTC_TRADING_VOL": {"type": "next_sibling", "text": "Monthly BTC Trading Vol"},
    "BINANCE_TRADING_DOMINANCE": {"type": "next_sibling", "text": "Binance Trading Dominance"},
    "BTC_PAIRS_TRADING_DOMINANCE": {"type": "next_sibling", "text": "BTC Pairs Trading Dominance"},
    "US_CRYPTO_TRADING_VOL": {"type": "next_sibling", "text": "US Crypto Trading Vol"},
    "OFFSHORE_CRYPTO_TRADING_VOL": {"type": "next_sibling", "text": "Offshore Crypto Trading Vol"},
    
    # ===== PRICE PERFORMANCE =====
    "DAILY_PRICE_PERFORMANCE": "#daily_price_performance",
    "WEEKLY_PRICE_PERFORMANCE": "#weekly_price_performance",
    "MONTHLY_PRICE_PERFORMANCE": "#monthly_price_performance",
    "QUARTERLY_PRICE_PERFORMANCE": "#quarterly_price_performance",
    
    # ===== BITCOIN VS GOLD =====
    "GOLD_PRICE": {"type": "next_sibling", "text": "Gold Price"},
    "GOLD_MARKETCAP": {"type": "next_sibling", "text": "Gold Marketcap"},
    "BTC_VS_GOLD_MARKETCAP": {"type": "next_sibling", "text": "Bitcoin vs Gold Market Cap"},
    "GOLD_CORRELATION": {"type": "next_sibling", "text": "Gold Correlation"},
    "GOLD_SUPPLY_TONNES": {"type": "next_sibling", "text": "Gold Supply in Tonnes"},
    
    # ===== REALIZED PRICE =====
    "REALIZED_PRICE": {"type": "next_sibling", "text": "Realized Price"},
    "REALIZED_MARKETCAP": {"type": "next_sibling", "text": "Realized Marketcap"},
    "STH_REALIZED_PRICE": {"type": "next_sibling", "text": "STH Realized Price"},
    "LTH_REALIZED_PRICE": {"type": "next_sibling", "text": "LTH Realized Price"},
    
    # ===== ADDRESS BALANCES =====
    "NEW_ADDRESSES": {"type": "next_sibling", "text": "New Addresses"},
    "BALANCE_1SAT_TO_001BTC": {"type": "next_sibling", "text": "1 sat to .01 BTC"},
    "BALANCE_001_TO_1BTC": {"type": "next_sibling", "text": ".01 BTC to 1 BTC"},
    "BALANCE_1_TO_10BTC": {"type": "next_sibling", "text": "1 BTC to 10 BTC"},
    "BALANCE_10_TO_100BTC": {"type": "next_sibling", "text": "10 BTC to 100 BTC"},
    "BALANCE_100_TO_1000BTC": {"type": "next_sibling", "text": "100 BTC to 1,000 BTC"},
    
    # ===== CORRELATIONS =====
    "CORRELATION_SPX": {"type": "next_sibling", "text": "SPDR S&P 500 - SPY"},
    "CORRELATION_GOLD": {"type": "next_sibling", "text": "SPDR Gold Trust - GLD"},
    "CORRELATION_IWM": {"type": "next_sibling", "text": "iShares Russell 2000 - IWM"},
    "CORRELATION_QQQ": {"type": "next_sibling", "text": "Invesco QQQ Trust - QQQ"},
    "CORRELATION_TLT": {"type": "next_sibling", "text": "iShares 20+ Year Treasury Bond - TLT"},
    
    # ===== ONCHAIN SUPPLY =====
    "LONG_TERM_HOLDER_SUPPLY": {"type": "next_sibling", "text": "Long Term Holder Supply"},
    "SHORT_TERM_HOLDER_SUPPLY": {"type": "next_sibling", "text": "Short Term Holder Supply"},
    "PERCENT_SUPPLY_IN_PROFIT": {"type": "next_sibling", "text": "Percent Supply in Profit"},
    "TOTAL_SUPPLY_IN_PROFIT": {"type": "next_sibling", "text": "Total Supply in Profit"},
    "TOTAL_SUPPLY_IN_LOSS": {"type": "next_sibling", "text": "Total Supply in Loss"},
    
    # ===== HALVING =====
    "PROJECTED_HALVING_DATE": {"type": "next_sibling", "text": "Projected Date"},
    "HALVING_AT_BLOCK": {"type": "next_sibling", "text": "Halving at Block"},
    "BLOCKS_REMAINING": {"type": "next_sibling", "text": "Blocks Remaining"},
    "BTC_UNTIL_HALVING": {"type": "next_sibling", "text": "BTC Until Halving"},
    "CURRENT_EPOCH_PCT": {"type": "next_sibling", "text": "Current Epoch %"},
    
    # ===== ONCHAIN METRICS =====
    "COIN_DAYS_DESTROYED": {"type": "next_sibling", "text": "Coin Days Destroyed"},
    "MVRV_Z_SCORE": {"type": "next_sibling", "text": "MVRV Z-Score"},
    "NVT_RATIO": {"type": "next_sibling", "text": "NVT Ratio"},
    "RHODL_RATIO": {"type": "next_sibling", "text": "RHODL Ratio"},
    "RESERVE_RISK": {"type": "next_sibling", "text": "Reserve Risk"},
    "VDD_MULTIPLE": {"type": "next_sibling", "text": "VDD Multiple"},
    "NET_REALIZED_PROFIT_LOSS": {"type": "next_sibling", "text": "Net Realized Profit Loss"},
    "NUPL": {"type": "next_sibling", "text": "NUPL"},
    "ASOL": {"type": "next_sibling", "text": "ASOL"},
    "MSOL": {"type": "next_sibling", "text": "MSOL"},
    
    # ===== NODES =====
    "TOTAL_NODES": {"type": "next_sibling", "text": "Total Nodes"},
    "TOR_NODES": {"type": "next_sibling", "text": "Tor Nodes"},
    "US_NODES": {"type": "next_sibling", "text": "United States of America"},
    "GERMANY_NODES": {"type": "next_sibling", "text": "Germany"},
    "FRANCE_NODES": {"type": "next_sibling", "text": "France"},
    "CANADA_NODES": {"type": "next_sibling", "text": "Canada"},
    "FINLAND_NODES": {"type": "next_sibling", "text": "Finland"},
    "NETHERLANDS_NODES": {"type": "next_sibling", "text": "Netherlands"},
    "UK_NODES": {"type": "next_sibling", "text": "United Kingdom"},
    "SWITZERLAND_NODES": {"type": "next_sibling", "text": "Switzerland"},
    "AUSTRALIA_NODES": {"type": "next_sibling", "text": "Australia"},
    "RUSSIA_NODES": {"type": "next_sibling", "text": "Russia"},
    
    # ===== FUTURES OPEN INTEREST =====
    "TOTAL_OPEN_INTEREST": {"type": "next_sibling", "text": "Total Open Interest"},
    "BINANCE_OI": {"type": "css", "selector": "#derivatives-box .dashboard-subcol:nth-child(2) p:last-child"},
    "OKX_OI": {"type": "css", "selector": "#derivatives-box .dashboard-subcol:nth-child(3) p:last-child"},
    "DERIBIT_OI": {"type": "css", "selector": "#derivatives-box .dashboard-subcol:nth-child(4) p:last-child"},
    "BYBIT_OI": {"type": "css", "selector": "#derivatives-box .dashboard-subcol:nth-child(5) p:last-child"},
    "BITMEX_OI": {"type": "css", "selector": "#derivatives-box .dashboard-subcol:nth-child(6) p:last-child"},
    "BITGET_OI": {"type": "css", "selector": "#derivatives-box .dashboard-subcol:nth-child(7) p:last-child"},
    "CRYPTOCOM_OI": {"type": "css", "selector": "#derivatives-box .dashboard-subcol:nth-child(8) p:last-child"},
    "KUCOIN_OI": {"type": "css", "selector": "#derivatives-box .dashboard-subcol:nth-child(9) p:last-child"},
    "GATEIO_OI": {"type": "css", "selector": "#derivatives-box .dashboard-subcol:nth-child(10) p:last-child"},
    "HUOBI_OI": {"type": "css", "selector": "#derivatives-box .dashboard-subcol:nth-child(11) p:last-child"},
    "BITFINEX_OI": {"type": "css", "selector": "#derivatives-box .dashboard-subcol:nth-child(12) p:last-child"},
    "KRAKEN_OI": {"type": "css", "selector": "#derivatives-box .dashboard-subcol:nth-child(13) p:last-child"},
    
    # ===== ETF TRADING 24H =====
    "SPOT_TRADING_VOLUME": {"type": "next_sibling", "text": "Spot Trading Volume"},
    "FUTURES_TRADING_VOLUME": {"type": "next_sibling", "text": "Futures Trading Volume"},
    "TOTAL_SPOT_AUM": {"type": "next_sibling", "text": "Total Spot AUM"},
    "TOTAL_BTC_HOLDINGS": {"type": "next_sibling", "text": "Total BTC Holdings"},
}

# Table selectors for dynamic content
TABLE_SELECTORS = {
    "ADDRESS_DISTRIBUTION": {
        "find_method": "text_contains",
        "search_text": "Category",
        "parent_levels": 2,  # Go up to table container
        "columns": ["category", "today", "1d_change", "1w_change", "1m_change", "1y_change", "1y_change_pct"]
    },
    "POLYMARKET_PREDICTIONS": {
        "find_method": "text_contains",
        "search_text": "Price Target",
        "parent_levels": 2,
        "columns": ["price_target", "probability", "volume_total", "volume_24h", "liquidity"]
    },
}