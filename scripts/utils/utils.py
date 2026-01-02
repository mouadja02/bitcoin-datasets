import re
from datetime import datetime
from bs4 import BeautifulSoup

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
    """Extracts text based on CSS selectors, supporting the special 'contains' pseudo-selector."""
    try:
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
            element = soup.select_one(selector)
            return element.get_text().strip() if element else None
    except Exception as e:
        print(f"Error extracting {selector}: {e}")
        return None

def scrape_table(soup, table_config):
    """Scrapes a table dynamically based on configuration."""
    try:
        container = soup.select_one(table_config["container"])
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
        print(f"Error scraping table {table_config.get('container')}: {e}")
        return []