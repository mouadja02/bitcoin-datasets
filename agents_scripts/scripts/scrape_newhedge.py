```python
#!/usr/bin/env python3
"""
Main scraping script for newhedge.io Bitcoin Dashboard using Firecrawl.
Extracts all metrics, cleans data, saves to 27 CSV files and raw JSON.
"""

import os
import json
import time
from datetime import datetime
from typing import Dict, Any
import pandas as pd
from bs4 import BeautifulSoup
from firecrawl import FirecrawlApp  # pip install firecrawl-py
from utils.selectors import SELECTORS, TABLE_SELECTORS

# Firecrawl API Key (set as env var)
API_KEY = os.getenv('FIRECRAWL_API_KEY')
if not API_KEY:
    raise ValueError("Set FIRECRAWL_API_KEY environment variable")

app = FirecrawlApp(api_key=API_KEY)

def parse_currency(value: str) -> float:
    """Parse currency like $89,817.93K -> 89817.93 * 1000"""
    value = value.replace('$', '').replace(',', '').strip()
    if 'T' in value:
        return float(value.replace('T', '')) * 1e12
    elif 'B' in value:
        return float(value.replace('B', '')) * 1e9
    elif 'M' in value:
        return float(value.replace('M', '')) * 1e6
    elif 'K' in value:
        return float(value.replace('K', '')) * 1e3
    return float(value)

def parse_percentage(value: str) -> float:
    """Parse % like +1.56% -> 1.56"""
    return float(value.strip('%').replace('+', '').replace('-', ''))

def parse_number(value: str) -> float:
    """General number parser, handle commas."""
    return float(value.replace(',', ''))

def extract_metrics(soup: BeautifulSoup) -> Dict[str, Any]:
    """Extract all individual metrics using selectors."""
    data = {}
    timestamp = datetime.utcnow().isoformat()
    data['timestamp'] = timestamp

    for key, selector in SELECTORS.items():
        elem = soup.select_one(selector)
        if elem:
            text = elem.get_text(strip=True)
            if 'price' in key or 'usd' in key.lower():
                data[key] = parse_currency(text)
            elif '%' in text:
                data[key] = parse_percentage(text)
            else:
                data[key] = parse_number(text)
        else:
            data[key] = None
    return data

def extract_table(table_selector: str, soup: BeautifulSoup, columns: list) -> pd.DataFrame:
    """Extract table data."""
    rows = soup.select(table_selector)
    table_data = []
    for row in rows:
        cells = [cell.get_text(strip=True) for cell in row.select('td')]
        if len(cells) >= len(columns):
            row_dict = dict(zip(columns, cells))
            row_dict['timestamp'] = datetime.utcnow().isoformat()
            table_data.append(row_dict)
    return pd.DataFrame(table_data)

def main():
    # Scrape page with Firecrawl
    scrape_url = "https://newhedge.io/bitcoin"
    scraped_data = app.scrape_url(scrape_url, params={'formats': ['markdown', 'html']})
    html = scraped_data['html']
    soup = BeautifulSoup(html, 'html.parser')

    # Extract metrics
    metrics = extract_metrics(soup)
    
    # Raw JSON
    with open('raw_newhedge_data.json', 'w') as f:
        json.dump({'metrics': metrics, 'html_length': len(html)}, f, indent=2)

    # Define CSV outputs (27 categories)
    csv_files = {
        'price_metrics': ['live_price', 'price_change_pct', '24h_high', '24h_low', '24h_vol_btc', '24h_vol_usd', 'ath_usd', 'market_cap', 'open_interest', 'btc_dominance'],
        'blockchain_metrics': ['block_height', 'block_speed', 'blocks_24h', 'outputs_24h'],
        'fear_greed': ['fear_greed_value', 'fear_greed_label'],
        'mining_metrics': ['hashrate', 'hashprice'],
        'fees_metrics': ['fees_per_tx_sats', 'fees_per_tx_usd', 'fees_24h_btc', 'fees_24h_usd'],
        'supply_metrics': ['circulating_supply', 'percent_issued', 'issuance_remaining', 'daily_issuance'],
        'corporate_treasuries': ['public_companies', 'public_holdings_btc', 'private_companies', 'private_holdings_btc', 'total_corp_holdings_btc', 'corp_supply_pct'],
        'gov_treasuries': ['gov_count', 'gov_btc', 'gov_usd', 'gov_supply_pct'],
        'transactions': ['tx_per_sec', 'tx_per_block', 'tx_per_day', 'total_tx'],
        'utxos': ['utxos_profit', 'utxos_loss', 'utxos_profit_pct'],
        'profitable_days': ['total_days', 'profitable_days', 'unprofitable_days', 'profitable_pct'],
        'macro_liquidity': ['global_m2', 'global_m2_growth', 'global_m2_yoy', 'us_m2', 'fed_funds_rate'],
        'ath_details': ['ath_price', 'drawdown_since_ath', 'days_since_ath'],
        'trading_volumes': ['daily_btc_vol', 'monthly_btc_vol', 'us_crypto_vol', 'offshore_crypto_vol'],
        'price_performance': ['daily_perf', 'weekly_perf', 'monthly_perf', 'quarterly_perf'],
        'gold_comparison': ['gold_price', 'gold_mcap', 'btc_vs_gold_mcap', 'gold_correlation'],
        'realized_prices': ['realized_price', 'realized_mcap', 'sth_realized', 'lth_realized'],
        'address_balances': ['new_addresses'],
        'onchain_supply': ['lth_supply', 'sth_supply', 'supply_profit_pct'],
        'halving': ['halving_date', 'blocks_remaining'],
        'onchain_indicators': ['cdd', 'mvrv_z', 'nvt', 'rhodl'],
        'node_distribution': [],  # Full table
        'mining_pool_dominance': [],  # Pie chart data
        'futures_oi': [],  # List
        'spot_etf': [],  # List
        'public_companies_top10': [],  # List
        'private_companies_top10': [],  # List
    }

    os.makedirs('data', exist_ok=True)

    # Save metrics CSVs
    for cat, keys in csv_files.items():
        if keys:
            df = pd.DataFrame([metrics])
            df = df[['timestamp'] + [k for k in keys if k in metrics]]
            df.to_csv(f'data/{cat}.csv', index=False)
        else:
            # Handle full tables
            if cat == 'address_distribution':
                df = extract_table(TABLE_SELECTORS['address_distribution'], soup, ['category', 'today', '1d_change', '1w_change', '1m_change', '1y_change', '1y_pct'])
            elif cat == 'polymarket_predictions':
                df = extract_table(TABLE_SELECTORS['polymarket_predictions'], soup, ['price_target', 'probability', 'vol_total', 'vol_24h', 'liquidity'])
            # Add more table logic
            df.to_csv(f'data/{cat}.csv', index=False)

    print("Scraping complete. Data saved to 'data/' folder and raw_newhedge_data.json")

if __name__ == "__main__":
    main()
```
