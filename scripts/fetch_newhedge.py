import os
import re
import json
from datetime import datetime, timezone
import pandas as pd
from firecrawl import FirecrawlApp
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from utils.selectors import SELECTORS, TABLE_SELECTORS

# Load environment variables
load_dotenv()

# Configuration
FIRECRAWL_API_KEY = os.getenv('FIRECRAWL_API_KEY')
URL = "https://newhedge.io/bitcoin"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'newhedge')

def clean_extracted_value(value, key):
    """Post-process extracted values to clean up duplicates and errors."""
    if not value:
        return value
    
    # Remove label duplicates (e.g., "Total Days\n5,648" -> "5,648")
    if '\n' in value:
        parts = value.split('\n')
        # If the label is repeated, take the second part
        if len(parts) == 2 and parts[0] in key:
            return parts[1].strip()
        # Otherwise take the part that looks like a value (has numbers or $)
        for part in parts:
            if any(c.isdigit() or c in '$%' for c in part):
                return part.strip()
    
    return value

# ===== UTILITY FUNCTIONS =====

def clean_numeric_value(value):
    """Cleans numeric values by removing currency symbols, commas, percentages, and units."""
    if value is None or value == '':
        return None
    
    try:
        cleaned = str(value).strip()
        cleaned = re.sub(r'[$€£¥,]', '', cleaned)
        
        is_percentage = '%' in cleaned
        cleaned = cleaned.replace('%', '')
        
        multiplier = 1
        if 'T' in cleaned and 'BTC' not in cleaned:
            multiplier = 1_000_000_000_000
            cleaned = cleaned.replace('T', '')
        elif 'B' in cleaned and 'BTC' not in cleaned:
            multiplier = 1_000_000_000
            cleaned = cleaned.replace('B', '')
        elif 'M' in cleaned:
            multiplier = 1_000_000
            cleaned = cleaned.replace('M', '')
        elif 'K' in cleaned:
            multiplier = 1_000
            cleaned = cleaned.replace('K', '')
        
        if 'EH/s' in cleaned:
            cleaned = cleaned.replace('EH/s', '').strip()
        
        cleaned = re.sub(r'[^\d.\-]', '', cleaned)
        
        if not cleaned or cleaned == '.':
            return None
            
        result = float(cleaned) * multiplier
        return result
    except (ValueError, AttributeError):
        return None

def clean_integer_value(value):
    """Cleans integer values."""
    numeric = clean_numeric_value(value)
    return int(numeric) if numeric is not None else None

def clean_percentage(value):
    """Cleans percentage values."""
    return clean_numeric_value(value)

def parse_date(date_str):
    """Parses date strings in various formats."""
    if not date_str:
        return None
    
    try:
        date_str = date_str.strip()
        for fmt in ['%B %d, %Y', '%A, %B %d, %Y', '%b %d, %Y', '%A, %b %d, %Y']:
            try:
                cleaned = re.sub(r'\s+', ' ', date_str)
                return datetime.strptime(cleaned, fmt).date()
            except ValueError:
                continue
        return None
    except Exception:
        return None

def extract_btc_amount(value):
    """Extracts BTC amount from strings like '1,036,543.54 BTC'."""
    if not value:
        return None
    cleaned = value.replace('BTC', '').strip()
    return clean_numeric_value(cleaned)

def extract_usd_with_percentage(value):
    """Extracts USD amount and percentage from strings like '$1.72B (7.43%)'."""
    if not value:
        return None, None
    
    amount_match = re.search(r'[\$€£¥]?[\d,.]+(B|M|K|T)?', value)
    amount = clean_numeric_value(amount_match.group(0)) if amount_match else None
    
    pct_match = re.search(r'\(([\d.]+)%\)', value)
    percentage = float(pct_match.group(1)) if pct_match else None
    
    return amount, percentage

def extract_element(soup, selector):
    """Extracts text based on various selector types."""
    try:
        # Handle dictionary selectors (new format)
        if isinstance(selector, dict):
            selector_type = selector.get('type')
            
            if selector_type == 'css':
                # Direct CSS selector
                css_selector = selector.get('selector')
                element = soup.select_one(css_selector)
                return element.get_text().strip() if element else None
            
            elif selector_type == 'next_sibling':
                # Find element containing the text, then get next sibling's value
                search_text = selector.get('text')
                context = selector.get('context')  # Optional context to narrow search
                
                elements = soup.find_all(string=lambda x: x and search_text in x)
                
                for elem in elements:
                    # If context is provided, check if we're in the right section
                    if context:
                        # Check if context appears in any parent
                        parent_chain = []
                        p = elem.find_parent()
                        for _ in range(5):  # Check up to 5 levels up
                            if p:
                                parent_chain.append(p.get_text())
                                p = p.find_parent()
                        
                        # If context not found in parent chain, skip this element
                        if not any(context in pc for pc in parent_chain):
                            continue
                    
                    parent = elem.find_parent()
                    if parent:
                        # Try to find the next element with the value
                        next_elem = parent.find_next_sibling()
                        if next_elem:
                            text = next_elem.get_text().strip()
                            if text and text != search_text:
                                return text
                        # Try finding within same container
                        container = parent.find_parent()
                        if container:
                            # Look for dashboard-primary-text or similar
                            value_elem = container.find(class_='dashboard-primary-text')
                            if value_elem:
                                return value_elem.get_text().strip()
                return None
            
            elif selector_type == 'dashboard_primary':
                # Find dashboard primary text in context
                context = selector.get('context')
                # Find section containing context
                section = soup.find(string=lambda x: x and context in x)
                if section:
                    container = section.find_parent().find_parent()
                    if container:
                        primary = container.find(class_='dashboard-primary-text')
                        if primary:
                            return primary.get_text().strip()
                return None
            
            elif selector_type == 'dashboard_secondary':
                # Find dashboard secondary text in context
                context = selector.get('context')
                section = soup.find(string=lambda x: x and context in x)
                if section:
                    container = section.find_parent().find_parent()
                    if container:
                        secondary = container.find(class_='dashboard-secondary-text')
                        if secondary:
                            return secondary.get_text().strip()
                return None
        
        # Handle string selectors (old format with CSS or :contains)
        elif isinstance(selector, str):
            if ":contains" in selector:
                parts = selector.split(":contains")
                base_tag = parts[0].strip() or "p"
                
                match = re.search(r"['\"](.*?)['\"]", parts[1])
                if not match:
                    return None
                search_text = match.group(1)
                
                elements = soup.select(base_tag) if base_tag else soup.find_all()
                
                for el in elements:
                    if search_text in el.get_text():
                        if "+ p" in selector:
                            sibling = el.find_next_sibling('p')
                            if sibling:
                                return sibling.get_text().strip()
                        else:
                            return el.get_text().strip()
                return None
            else:
                # Standard CSS selector
                element = soup.select_one(selector)
                return element.get_text().strip() if element else None
            
        return None
    except Exception as e:
        # print(f"Error extracting {selector}: {e}")
        return None

def scrape_table(soup, table_config):
    """Scrapes a table dynamically based on configuration."""
    try:
        find_method = table_config.get("find_method")
        
        if find_method == "id":
            # Direct table ID lookup
            table_id = table_config.get("table_id")
            table = soup.find('table', id=table_id)
            
            if not table:
                return []
            
            # Extract rows
            tbody = table.find('tbody')
            if not tbody:
                rows = table.find_all('tr')
            else:
                rows = tbody.find_all('tr')
            
            data = []
            
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) > 0:
                    row_data = {}
                    columns = table_config["columns"]
                    for i, col_name in enumerate(columns):
                        if i < len(cells):
                            row_data[col_name] = cells[i].get_text().strip()
                        else:
                            row_data[col_name] = None
                    data.append(row_data)
            
            return data
        
        elif find_method == "text_contains":
            search_text = table_config.get("search_text")
            parent_levels = table_config.get("parent_levels", 2)
            
            # Find element containing the search text (header)
            header = soup.find(string=lambda x: x and search_text == x.strip())
            if not header:
                # Try partial match
                header = soup.find(string=lambda x: x and search_text in x)
            
            if not header:
                return []
            
            # Navigate up to find the table container
            container = header
            for _ in range(parent_levels):
                container = container.find_parent()
                if not container:
                    return []
            
            # Find the table within or after the container
            table = container.find('table')
            if not table:
                # Try finding table as next sibling
                table = container.find_next('table')
            
            if not table:
                return []
            
            # Extract rows
            rows = table.find_all('tr')
            if len(rows) < 2:  # Need at least header + 1 data row
                return []
            
            data = []
            
            # Skip header row
            for row in rows[1:]:
                cells = row.find_all(['td', 'th'])
                if len(cells) > 0:
                    row_data = {}
                    columns = table_config["columns"]
                    for i, col_name in enumerate(columns):
                        if i < len(cells):
                            row_data[col_name] = cells[i].get_text().strip()
                        else:
                            row_data[col_name] = None
                    data.append(row_data)
            
            return data
        
        # Old method (container selector)
        else:
            container = soup.select_one(table_config.get("container", ""))
            if not container:
                return []
            
            rows = container.select(table_config["rows"])
            data = []
            
            for row in rows[1:]:  # Skip header row
                cells = row.find_all(['td', 'th'])
                if len(cells) >= len(table_config["columns"]):
                    row_data = {}
                    for i, col_name in enumerate(table_config["columns"]):
                        row_data[col_name] = cells[i].get_text().strip()
                    data.append(row_data)
            
            return data
    except Exception as e:
        print(f"Error scraping table: {e}")
        import traceback
        traceback.print_exc()
        return []

# ===== MAIN SCRAPING FUNCTION =====

def fetch_data():
    if not FIRECRAWL_API_KEY:
        print("Error: FIRECRAWL_API_KEY not found.")
        return

    print("Initializing Firecrawl...")
    app = FirecrawlApp(api_key=FIRECRAWL_API_KEY)
    
    print(f"Scraping {URL}...")
    try:
        scrape_result = app.scrape(URL, formats=['html'])
        html_content = scrape_result.html if hasattr(scrape_result, 'html') else None
        
        if not html_content:
            print("No HTML content returned.")
            return

        soup = BeautifulSoup(html_content, 'html.parser')
        timestamp = datetime.now(timezone.utc)
        
        # Extract all raw data points
        print("Extracting data points...")
        raw_data = {}
        for key, selector in SELECTORS.items():
            value = extract_element(soup, selector)
            # Clean up the value
            value = clean_extracted_value(value, key)
            raw_data[key] = value
            if value:
                print(f"  {key}: {value}")
        
        # Extract table data
        print("\nExtracting table data...")
        scraped_tables = {}
        for table_name, table_config in TABLE_SELECTORS.items():
            scraped_tables[table_name] = scrape_table(soup, table_config)
            print(f"  {table_name}: {len(scraped_tables[table_name])} rows")
        
        # ===== STRUCTURED DATA TABLES =====
        
        # 1. Market Overview
        market_overview = {
            'TIMESTAMP': timestamp,
            '24H_HIGH': clean_numeric_value(raw_data.get('24H_HIGH')),
            '24H_LOW': clean_numeric_value(raw_data.get('24H_LOW')),
            '24H_VOL_BTC': clean_numeric_value(raw_data.get('24H_VOL_BTC')),
            '24H_VOL_USD': clean_numeric_value(raw_data.get('24H_VOL_USD')),
            'LIVE_PRICE': clean_numeric_value(raw_data.get('LIVE_PRICE')),
            'MARKET_CAP': clean_numeric_value(raw_data.get('MARKET_CAP')),
            'BTC_DOMINANCE_PCT': clean_percentage(raw_data.get('BTC_DOMINANCE')),
            'SATS_PER_DOLLAR': clean_integer_value(raw_data.get('SATS_PER_DOLLAR'))
        }
        
        # 2. Blockchain Metrics
        blockchain_metrics = {
            'TIMESTAMP': timestamp,
            'BLOCK_HEIGHT': clean_integer_value(raw_data.get('BLOCK_HEIGHT')),
            'TIME_SINCE_LAST_BLOCK': raw_data.get('TIME_SINCE_LAST_BLOCK'),
            'BLOCK_SPEED': clean_numeric_value(raw_data.get('BLOCK_SPEED')),
            'BLOCKS_24HRS': clean_integer_value(raw_data.get('BLOCKS_24HRS')),
            'OUTPUTS_24HRS': clean_integer_value(raw_data.get('OUTPUTS_24HRS'))
        }
        
        # 3. Difficulty Adjustment
        difficulty_adjustment = {
            'TIMESTAMP': timestamp,
            'PREVIOUS_DIFFICULTY': clean_numeric_value(raw_data.get('PREVIOUS_DIFFICULTY')),
            'PREVIOUS_DIFFICULTY_CHANGE_PCT': clean_percentage(raw_data.get('PREVIOUS_DIFFICULTY_CHANGE')),
            'CURRENT_DIFFICULTY': clean_numeric_value(raw_data.get('CURRENT_DIFFICULTY')),
            'NEXT_DIFFICULTY_ESTIMATE': clean_numeric_value(raw_data.get('NEXT_DIFFICULTY_ESTIMATE')),
            'NEXT_DIFFICULTY_CHANGE_PCT': clean_percentage(raw_data.get('NEXT_DIFFICULTY_CHANGE_PCT')),
            'NEXT_RETARGET': raw_data.get('NEXT_RETARGET'),
            'EPOCH': clean_integer_value(raw_data.get('EPOCH'))
        }
        
        # 4. Fear & Greed Index
        fear_greed = {
            'TIMESTAMP': timestamp,
            'INDEX_VALUE': clean_numeric_value(raw_data.get('FEAR_GREED_INDEX')),
            'LABEL': raw_data.get('FEAR_GREED_LABEL')
        }
        
        # 5. Mining Metrics
        mining_metrics = {
            'TIMESTAMP': timestamp,
            'HASHRATE_EHS': clean_numeric_value(raw_data.get('HASHRATE')),
            'HASHPRICE_USD': clean_numeric_value(raw_data.get('HASHPRICE')),
            'REVENUE_BTC_24H': clean_numeric_value(raw_data.get('REVENUE_BTC_24HRS')),
            'REVENUE_USD_24H': clean_numeric_value(raw_data.get('REVENUE_USD_24HRS')),
            'REWARD_PER_BLOCK_BTC': clean_numeric_value(raw_data.get('REWARD_PER_BLOCK_BTC')),
            'REWARD_PER_BLOCK_USD': clean_numeric_value(raw_data.get('REWARD_PER_BLOCK_USD')),
            'REWARD_BTC_24HRS': clean_numeric_value(raw_data.get('REWARD_BTC_24HRS')),
            'REWARD_USD_24HRS': clean_numeric_value(raw_data.get('REWARD_USD_24HRS')),
            'FEES_VS_REWARD_PCT': clean_percentage(raw_data.get('FEES_VS_REWARD_PCT')),
            'CURRENT_MONTH_SUBSIDY_USD': clean_numeric_value(raw_data.get('CURRENT_MONTH_SUBSIDY_USD')),
            'CURRENT_MONTH_FEES_USD': clean_numeric_value(raw_data.get('CURRENT_MONTH_FEES_USD')),
            'CURRENT_MONTH_TOTAL_USD': clean_numeric_value(raw_data.get('CURRENT_MONTH_TOTAL_USD'))
        }
        
        # 6. Fees
        fee_metrics = {
            'TIMESTAMP': timestamp,
            'PER_TRANSACTION_SATS': clean_numeric_value(raw_data.get('PER_TRANSACTION_SATS')),
            'PER_TRANSACTION_USD': clean_numeric_value(raw_data.get('PER_TRANSACTION_USD')),
            'FEES_BTC_24HRS': clean_numeric_value(raw_data.get('FEES_BTC_24HRS')),
            'FEES_USD_24HRS': clean_numeric_value(raw_data.get('FEES_USD_24HRS'))
        }
        
        # 7. Supply Metrics
        supply_metrics = {
            'TIMESTAMP': timestamp,
            'CIRCULATING_SUPPLY': clean_numeric_value(raw_data.get('CIRCULATING_SUPPLY')),
            'PERCENTAGE_ISSUED_PCT': clean_percentage(raw_data.get('PERCENTAGE_ISSUED')),
            'ISSUANCE_REMAINING': clean_numeric_value(raw_data.get('ISSUANCE_REMAINING')),
            'TOTAL_MINED_BLOCKS': clean_integer_value(raw_data.get('TOTAL_MINED_BLOCKS')),
            'ISSUANCE_BTC_24HRS': clean_numeric_value(raw_data.get('ISSUANCE_BTC_24HRS'))
        }
        
        # 8. Corporate Holdings
        corporate_holdings = {
            'TIMESTAMP': timestamp,
            'PUBLIC_COMPANIES_COUNT': clean_integer_value(raw_data.get('PUBLIC_COMPANIES_COUNT')),
            'PUBLIC_HOLDINGS_BTC': clean_numeric_value(raw_data.get('PUBLIC_HOLDINGS_BTC')),
            'PUBLIC_HOLDINGS_USD': clean_numeric_value(raw_data.get('PUBLIC_HOLDINGS_USD')),
            'PRIVATE_COMPANIES_COUNT': clean_integer_value(raw_data.get('PRIVATE_COMPANIES_COUNT')),
            'PRIVATE_HOLDINGS_BTC': clean_numeric_value(raw_data.get('PRIVATE_HOLDINGS_BTC')),
            'PRIVATE_HOLDINGS_USD': clean_numeric_value(raw_data.get('PRIVATE_HOLDINGS_USD')),
            'TOTAL_CORPORATE_BTC': clean_numeric_value(raw_data.get('TOTAL_CORPORATE_BTC')),
            'TOTAL_CORPORATE_USD': clean_numeric_value(raw_data.get('TOTAL_CORPORATE_USD')),
            'CORPORATE_PCT_TOTAL_SUPPLY': clean_percentage(raw_data.get('CORPORATE_PCT_TOTAL_SUPPLY'))
        }
        
        # 9. Government Holdings
        government_holdings = {
            'TIMESTAMP': timestamp,
            'GOVERNMENTS_COUNT': clean_integer_value(raw_data.get('GOVERNMENTS_COUNT')),
            'GOVERNMENT_TREASURY_BTC': clean_numeric_value(raw_data.get('GOVERNMENT_BTC_TREASURIES')),
            'GOVERNMENT_TREASURY_USD': clean_numeric_value(raw_data.get('GOVERNMENT_USD_TREASURIES')),
            'GOVERNMENT_PCT_TOTAL_SUPPLY': clean_percentage(raw_data.get('GOVERNMENT_PCT_TOTAL_SUPPLY'))
        }
        
        # 10. Transaction Metrics
        transaction_metrics = {
            'TIMESTAMP': timestamp,
            'TRANSACTIONS_PER_SECOND': clean_numeric_value(raw_data.get('TRANSACTIONS_PER_SECOND')),
            'TRANSACTIONS_PER_BLOCK': clean_numeric_value(raw_data.get('TRANSACTIONS_PER_BLOCK')),
            'TRANSACTIONS_PER_DAY': clean_numeric_value(raw_data.get('TRANSACTIONS_PER_DAY')),
            'TRANSACTIONS_CURRENT_MONTH': clean_numeric_value(raw_data.get('TRANSACTIONS_CURRENT_MONTH')),
            'TOTAL_TRANSACTIONS_ALL_TIME': clean_integer_value(raw_data.get('TOTAL_TRANSACTIONS_ALL_TIME'))
        }
        
        # 11. UTXO Metrics
        utxo_metrics = {
            'TIMESTAMP': timestamp,
            'UTXOS_IN_PROFIT': clean_numeric_value(raw_data.get('UTXOS_IN_PROFIT')),
            'UTXOS_IN_LOSS': clean_numeric_value(raw_data.get('UTXOS_IN_LOSS')),
            'UTXOS_IN_PROFIT_PCT': clean_percentage(raw_data.get('UTXOS_IN_PROFIT_PCT'))
        }
        
        # 12. Profitable Days
        profitable_days = {
            'TIMESTAMP': timestamp,
            'TOTAL_DAYS': clean_integer_value(raw_data.get('TOTAL_DAYS')),
            'PROFITABLE_DAYS': clean_integer_value(raw_data.get('PROFITABLE_DAYS')),
            'UNPROFITABLE_DAYS': clean_integer_value(raw_data.get('UNPROFITABLE_DAYS')),
            'PERCENTAGE_PROFITABLE_PCT': clean_percentage(raw_data.get('PERCENTAGE_PROFITABLE'))
        }
        
        # 13. Macro & Liquidity
        macro_liquidity = {
            'TIMESTAMP': timestamp,
            'GLOBAL_M2_SUPPLY': clean_numeric_value(raw_data.get('GLOBAL_M2_SUPPLY')),
            'GLOBAL_M2_GROWTH': clean_numeric_value(raw_data.get('GLOBAL_M2_GROWTH')),
            'GLOBAL_M2_YOY_GROWTH_PCT': clean_percentage(raw_data.get('GLOBAL_M2_YOY_GROWTH')),
            'GLOBAL_M2_10WEEK_LEAD': clean_numeric_value(raw_data.get('GLOBAL_M2_10WEEK_LEAD')),
            'US_M2_SUPPLY': clean_numeric_value(raw_data.get('US_M2_SUPPLY')),
            'FEDERAL_FUNDS_RATE_PCT': clean_percentage(raw_data.get('FEDERAL_FUNDS_RATE'))
        }
        
        # 14. ATH Details
        ath_details = {
            'TIMESTAMP': timestamp,
            'ATH_PRICE_USD': clean_numeric_value(raw_data.get('ATH_PRICE')),
            'ATH_DATE': parse_date(raw_data.get('ATH_DATE')),
            'DAYS_SINCE_ATH': clean_integer_value(raw_data.get('DAYS_SINCE_ATH')),
            'PRICE_DRAWDOWN_PCT': clean_percentage(raw_data.get('PRICE_DRAWDOWN_SINCE_ATH'))
        }
        
        # 15. Trading Metrics
        us_vol, us_pct = extract_usd_with_percentage(raw_data.get('US_CRYPTO_TRADING_VOL'))
        offshore_vol, offshore_pct = extract_usd_with_percentage(raw_data.get('OFFSHORE_CRYPTO_TRADING_VOL'))
        
        trading_metrics = {
            'TIMESTAMP': timestamp,
            'DAILY_BTC_TRADING_VOL_USD': clean_numeric_value(raw_data.get('DAILY_BTC_TRADING_VOL')),
            'MONTHLY_BTC_TRADING_VOL_USD': clean_numeric_value(raw_data.get('MONTHLY_BTC_TRADING_VOL')),
            'BINANCE_DOMINANCE_PCT': clean_percentage(raw_data.get('BINANCE_TRADING_DOMINANCE')),
            'BTC_PAIRS_DOMINANCE_PCT': clean_percentage(raw_data.get('BTC_PAIRS_TRADING_DOMINANCE')),
            'US_TRADING_VOL_USD': us_vol,
            'US_TRADING_VOL_PCT': us_pct,
            'OFFSHORE_TRADING_VOL_USD': offshore_vol,
            'OFFSHORE_TRADING_VOL_PCT': offshore_pct
        }
        
        # 16. Price Performance
        price_performance = {
            'TIMESTAMP': timestamp,
            'DAILY_PERFORMANCE_PCT': clean_percentage(raw_data.get('DAILY_PRICE_PERFORMANCE')),
            'WEEKLY_PERFORMANCE_PCT': clean_percentage(raw_data.get('WEEKLY_PRICE_PERFORMANCE')),
            'MONTHLY_PERFORMANCE_PCT': clean_percentage(raw_data.get('MONTHLY_PRICE_PERFORMANCE')),
            'QUARTERLY_PERFORMANCE_PCT': clean_percentage(raw_data.get('QUARTERLY_PRICE_PERFORMANCE'))
        }
        
        # 17. Gold Comparison
        gold_comparison = {
            'TIMESTAMP': timestamp,
            'GOLD_PRICE_USD': clean_numeric_value(raw_data.get('GOLD_PRICE')),
            'GOLD_MARKETCAP_USD': clean_numeric_value(raw_data.get('GOLD_MARKETCAP')),
            'BTC_VS_GOLD_MARKETCAP_PCT': clean_percentage(raw_data.get('BTC_VS_GOLD_MARKETCAP')),
            'GOLD_CORRELATION': clean_numeric_value(raw_data.get('GOLD_CORRELATION')),
            'GOLD_SUPPLY_TONNES': clean_numeric_value(raw_data.get('GOLD_SUPPLY_TONNES'))
        }
        
        # 18. Realized Price Metrics
        realized_price = {
            'TIMESTAMP': timestamp,
            'REALIZED_PRICE_USD': clean_numeric_value(raw_data.get('REALIZED_PRICE')),
            'REALIZED_MARKETCAP_USD': clean_numeric_value(raw_data.get('REALIZED_MARKETCAP')),
            'STH_REALIZED_PRICE_USD': clean_numeric_value(raw_data.get('STH_REALIZED_PRICE')),
            'LTH_REALIZED_PRICE_USD': clean_numeric_value(raw_data.get('LTH_REALIZED_PRICE'))
        }
        
        # 19. Address Balances
        address_balances = {
            'TIMESTAMP': timestamp,
            'NEW_ADDRESSES': clean_integer_value(raw_data.get('NEW_ADDRESSES')),
            'BALANCE_1SAT_TO_001BTC': clean_integer_value(raw_data.get('BALANCE_1SAT_TO_001BTC')),
            'BALANCE_001_TO_1BTC': clean_integer_value(raw_data.get('BALANCE_001_TO_1BTC')),
            'BALANCE_1_TO_10BTC': clean_integer_value(raw_data.get('BALANCE_1_TO_10BTC')),
            'BALANCE_10_TO_100BTC': clean_integer_value(raw_data.get('BALANCE_10_TO_100BTC')),
            'BALANCE_100_TO_1000BTC': clean_integer_value(raw_data.get('BALANCE_100_TO_1000BTC'))
        }
        
        # 20. Correlations
        correlations = {
            'TIMESTAMP': timestamp,
            'CORRELATION_SPX': clean_numeric_value(raw_data.get('CORRELATION_SPX')),
            'CORRELATION_GOLD': clean_numeric_value(raw_data.get('CORRELATION_GOLD')),
            'CORRELATION_IWM': clean_numeric_value(raw_data.get('CORRELATION_IWM')),
            'CORRELATION_QQQ': clean_numeric_value(raw_data.get('CORRELATION_QQQ')),
            'CORRELATION_TLT': clean_numeric_value(raw_data.get('CORRELATION_TLT'))
        }
        
        # 21. Onchain Supply
        onchain_supply = {
            'TIMESTAMP': timestamp,
            'LONG_TERM_HOLDER_SUPPLY': clean_numeric_value(raw_data.get('LONG_TERM_HOLDER_SUPPLY')),
            'SHORT_TERM_HOLDER_SUPPLY': clean_numeric_value(raw_data.get('SHORT_TERM_HOLDER_SUPPLY')),
            'SUPPLY_IN_PROFIT_PCT': clean_percentage(raw_data.get('PERCENT_SUPPLY_IN_PROFIT')),
            'TOTAL_SUPPLY_IN_PROFIT': clean_numeric_value(raw_data.get('TOTAL_SUPPLY_IN_PROFIT')),
            'TOTAL_SUPPLY_IN_LOSS': clean_numeric_value(raw_data.get('TOTAL_SUPPLY_IN_LOSS'))
        }
        
        # 22. Halving Metrics
        halving_metrics = {
            'TIMESTAMP': timestamp,
            'PROJECTED_HALVING_DATE': parse_date(raw_data.get('PROJECTED_HALVING_DATE')),
            'HALVING_BLOCK_HEIGHT': clean_integer_value(raw_data.get('HALVING_AT_BLOCK')),
            'BLOCKS_REMAINING': clean_integer_value(raw_data.get('BLOCKS_REMAINING')),
            'BTC_UNTIL_HALVING': clean_numeric_value(raw_data.get('BTC_UNTIL_HALVING')),
            'CURRENT_EPOCH_PCT': clean_percentage(raw_data.get('CURRENT_EPOCH_PCT'))
        }
        
        # 23. Onchain Indicators
        onchain_indicators = {
            'TIMESTAMP': timestamp,
            'COIN_DAYS_DESTROYED': clean_numeric_value(raw_data.get('COIN_DAYS_DESTROYED')),
            'MVRV_Z_SCORE': clean_numeric_value(raw_data.get('MVRV_Z_SCORE')),
            'NVT_RATIO': clean_numeric_value(raw_data.get('NVT_RATIO')),
            'RHODL_RATIO': clean_numeric_value(raw_data.get('RHODL_RATIO')),
            'RESERVE_RISK': clean_numeric_value(raw_data.get('RESERVE_RISK')),
            'VDD_MULTIPLE': clean_numeric_value(raw_data.get('VDD_MULTIPLE')),
            'NET_REALIZED_PROFIT_LOSS': clean_numeric_value(raw_data.get('NET_REALIZED_PROFIT_LOSS')),
            'NUPL': clean_numeric_value(raw_data.get('NUPL')),
            'ASOL': clean_numeric_value(raw_data.get('ASOL')),
            'MSOL': clean_numeric_value(raw_data.get('MSOL'))
        }
        
        # 24. Node Metrics
        node_metrics = {
            'TIMESTAMP': timestamp,
            'TOTAL_NODES': clean_integer_value(raw_data.get('TOTAL_NODES')),
            'TOR_NODES': clean_integer_value(raw_data.get('TOR_NODES')),
            'TOR_NODES_PCT': None,  # Calculate if needed
            'US_NODES': clean_integer_value(raw_data.get('US_NODES')),
            'GERMANY_NODES': clean_integer_value(raw_data.get('GERMANY_NODES')),
            'FRANCE_NODES': clean_integer_value(raw_data.get('FRANCE_NODES')),
            'CANADA_NODES': clean_integer_value(raw_data.get('CANADA_NODES')),
            'FINLAND_NODES': clean_integer_value(raw_data.get('FINLAND_NODES')),
            'NETHERLANDS_NODES': clean_integer_value(raw_data.get('NETHERLANDS_NODES')),
            'UK_NODES': clean_integer_value(raw_data.get('UK_NODES')),
            'SWITZERLAND_NODES': clean_integer_value(raw_data.get('SWITZERLAND_NODES')),
            'AUSTRALIA_NODES': clean_integer_value(raw_data.get('AUSTRALIA_NODES')),
            'RUSSIA_NODES': clean_integer_value(raw_data.get('RUSSIA_NODES'))
        }
        
        # 25. Futures Open Interest
        futures_oi = {
            'TIMESTAMP': timestamp,
            'TOTAL_OPEN_INTEREST': clean_numeric_value(raw_data.get('TOTAL_OPEN_INTEREST')),
            'BINANCE_OI': clean_numeric_value(raw_data.get('BINANCE_OI')),
            'OKX_OI': clean_numeric_value(raw_data.get('OKX_OI')),
            'DERIBIT_OI': clean_numeric_value(raw_data.get('DERIBIT_OI')),
            'BYBIT_OI': clean_numeric_value(raw_data.get('BYBIT_OI')),
            'BITMEX_OI': clean_numeric_value(raw_data.get('BITMEX_OI')),
            'BITGET_OI': clean_numeric_value(raw_data.get('BITGET_OI')),
            'CRYPTOCOM_OI': clean_numeric_value(raw_data.get('CRYPTOCOM_OI')),
            'KUCOIN_OI': clean_numeric_value(raw_data.get('KUCOIN_OI')),
            'GATEIO_OI': clean_numeric_value(raw_data.get('GATEIO_OI')),
            'HUOBI_OI': clean_numeric_value(raw_data.get('HUOBI_OI')),
            'BITFINEX_OI': clean_numeric_value(raw_data.get('BITFINEX_OI')),
            'KRAKEN_OI': clean_numeric_value(raw_data.get('KRAKEN_OI'))
        }
        
        # 26. ETF Trading
        etf_trading = {
            'TIMESTAMP': timestamp,
            'SPOT_TRADING_VOLUME': clean_numeric_value(raw_data.get('SPOT_TRADING_VOLUME')),
            'FUTURES_TRADING_VOLUME': clean_numeric_value(raw_data.get('FUTURES_TRADING_VOLUME')),
            'TOTAL_SPOT_AUM': clean_numeric_value(raw_data.get('TOTAL_SPOT_AUM')),
            'TOTAL_BTC_HOLDINGS': clean_numeric_value(raw_data.get('TOTAL_BTC_HOLDINGS'))
        }
        
        # 27. ETF Holdings (Major ones)
        etf_holdings = {
            'TIMESTAMP': timestamp,
            'IBIT_BLACKROCK_BTC': clean_numeric_value(raw_data.get('IBIT_BLACKROCK')),
            'FBTC_FIDELITY_BTC': clean_numeric_value(raw_data.get('FBTC_FIDELITY')),
            'GBTC_GRAYSCALE_BTC': clean_numeric_value(raw_data.get('GBTC_GRAYSCALE'))
        }
        
        # ===== SAVE TO CSV FILES =====
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        # Core metric tables
        tables = {
            'market_overview': market_overview,
            'blockchain_metrics': blockchain_metrics,
            'difficulty_adjustment': difficulty_adjustment,
            'fear_greed': fear_greed,
            'mining_metrics': mining_metrics,
            'fee_metrics': fee_metrics,
            'supply_metrics': supply_metrics,
            'corporate_holdings': corporate_holdings,
            'government_holdings': government_holdings,
            'transaction_metrics': transaction_metrics,
            'utxo_metrics': utxo_metrics,
            'profitable_days': profitable_days,
            'macro_liquidity': macro_liquidity,
            'ath_details': ath_details,
            'trading_metrics': trading_metrics,
            'price_performance': price_performance,
            'gold_comparison': gold_comparison,
            'realized_price': realized_price,
            'address_balances': address_balances,
            'correlations': correlations,
            'onchain_supply': onchain_supply,
            'halving_metrics': halving_metrics,
            'onchain_indicators': onchain_indicators,
            'node_metrics': node_metrics,
            'futures_oi': futures_oi,
            'etf_trading': etf_trading,
            'etf_holdings': etf_holdings,
        }
        
        print("\nSaving core metrics to CSV files...")
        for table_name, table_data in tables.items():
            output_file = os.path.join(OUTPUT_DIR, f'{table_name}.csv')
            file_exists = os.path.isfile(output_file)
            
            df = pd.DataFrame([table_data])
            df.to_csv(output_file, mode='a', header=not file_exists, index=False)
            print(f"  ✓ {table_name}.csv")
        
        # Save table data (companies, ETFs, etc.)
        print("\nSaving table data...")
        for table_name, data in scraped_tables.items():
            if data:
                output_file = os.path.join(OUTPUT_DIR, f'{table_name.lower()}.csv')
                
                # Add timestamp to each row
                for row in data:
                    row['TIMESTAMP'] = timestamp
                
                df = pd.DataFrame(data)
                file_exists = os.path.isfile(output_file)
                df.to_csv(output_file, mode='a', header=not file_exists, index=False)
                print(f"  ✓ {table_name.lower()}.csv ({len(data)} rows)")
        
        # Save raw data for debugging
        raw_output_file = os.path.join(OUTPUT_DIR, 'raw_data.json')
        with open(raw_output_file, 'w') as f:
            json.dump({
                'timestamp': timestamp.isoformat(),
                'raw_data': raw_data,
                'scraped_tables': {k: v for k, v in scraped_tables.items()}
            }, f, indent=2, default=str)
        print(f"  ✓ raw_data.json")
        
        print(f"\n✓ All data saved to {OUTPUT_DIR}")
        
    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    fetch_data()
