import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
NEWHEDGE_DIR = os.path.join(DATA_DIR, 'newhedge')

def get_snowflake_conn():
    """Create Snowflake connection."""
    try:
        conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema='NEWHEDGE'
        )
        return conn
    except Exception as e:
        print(f"Could not connect to Snowflake: {e}")
        return None

def load_csv_to_table(conn, csv_file, table_name, normalize_columns=True):
    """Load a CSV file into a Snowflake table."""
    try:
        df = pd.read_csv(csv_file)
        
        if df.empty:
            print(f"  ⚠️  {csv_file} is empty, skipping...")
            return False
        
        # Normalize column names
        if normalize_columns:
            df.columns = [c.upper().replace(' ', '_').replace('(', '').replace(')', '').replace('-', '_') for c in df.columns]
        
        # Convert timestamp columns
        if 'TIMESTAMP' in df.columns:
            df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'])
        
        # Use MERGE to upsert data (update if exists, insert if not)
        # For simplicity, we'll use write_pandas with overwrite=False (append mode)
        # In production, you'd want proper MERGE logic
        
        success, n_chunks, n_rows, _ = write_pandas(
            conn,
            df,
            table_name,
            auto_create_table=False,  # Tables already created by migration
            overwrite=False,  # Append mode
            quote_identifiers=False
        )
        
        if success:
            print(f"  ✓ Loaded {n_rows} rows into NEWHEDGE.{table_name}")
            return True
        else:
            print(f"  ✗ Failed to load into {table_name}")
            return False
            
    except Exception as e:
        print(f"  ✗ Error loading {csv_file} to {table_name}: {e}")
        return False

def merge_data_to_table(conn, df, table_name, merge_key='TIMESTAMP'):
    """Merge data into table using MERGE statement."""
    try:
        cursor = conn.cursor()
        
        # Create temp table
        temp_table = f"{table_name}_TEMP_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        # Write to temp table
        success, n_chunks, n_rows, _ = write_pandas(
            conn,
            df,
            temp_table,
            auto_create_table=True,
            quote_identifiers=False
        )
        
        if not success:
            print(f"  ✗ Failed to create temp table {temp_table}")
            return False
        
        # Build column list
        columns = df.columns.tolist()
        col_list = ', '.join(columns)
        update_list = ', '.join([f"TARGET.{col} = SOURCE.{col}" for col in columns if col != merge_key])
        insert_cols = ', '.join(columns)
        insert_vals = ', '.join([f"SOURCE.{col}" for col in columns])
        
        # Execute MERGE
        merge_sql = f"""
        MERGE INTO NEWHEDGE.{table_name} AS TARGET
        USING {temp_table} AS SOURCE
        ON TARGET.{merge_key} = SOURCE.{merge_key}
        WHEN MATCHED THEN
            UPDATE SET {update_list}
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols})
            VALUES ({insert_vals})
        """
        
        cursor.execute(merge_sql)
        rows_merged = cursor.rowcount
        
        # Drop temp table
        cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")
        
        print(f"  ✓ Merged {rows_merged} rows into NEWHEDGE.{table_name}")
        
        cursor.close()
        return True
        
    except Exception as e:
        print(f"  ✗ Error merging into {table_name}: {e}")
        return False

def load_newhedge_data(conn):
    """Load all NewHedge CSV files into Snowflake tables."""
    
    # Mapping of CSV files to table names
    file_table_mapping = {
        'market_overview.csv': 'MARKET_DATA',
        'blockchain_metrics.csv': 'BLOCKCHAIN_METRICS',
        'mining_metrics.csv': 'MINING_METRICS',
        'fee_metrics.csv': 'FEE_METRICS',
        'supply_metrics.csv': 'SUPPLY_METRICS',
        'corporate_holdings.csv': 'HOLDINGS',  # Need to merge corporate + government
        'government_holdings.csv': 'HOLDINGS',
        'trading_metrics.csv': 'TRADING_METRICS',
        'etf_trading.csv': 'TRADING_METRICS',  # Merge with trading metrics
        'etf_holdings.csv': 'TRADING_METRICS',  # Merge with trading metrics
        'futures_oi.csv': 'FUTURES_OPEN_INTEREST',
        'address_balances.csv': 'ADDRESS_METRICS',
        'address_distribution.csv': 'ADDRESS_DISTRIBUTION',
        'onchain_indicators.csv': 'ONCHAIN_INDICATORS',
        'realized_price.csv': 'REALIZED_PRICE',
        'profitable_days.csv': 'PROFITABLE_DAYS',
        'macro_liquidity.csv': 'MACRO_LIQUIDITY',
        'correlations.csv': 'CORRELATIONS',
        'gold_comparison.csv': 'GOLD_COMPARISON',
        'node_metrics.csv': 'NODE_METRICS',
        'fear_greed.csv': 'MARKET_DATA',  # Merge into market data
        'ath_details.csv': 'MARKET_DATA',  # Merge into market data
        'price_performance.csv': 'MARKET_DATA',  # Merge into market data
        'halving_metrics.csv': 'SUPPLY_METRICS',  # Merge into supply metrics
        'utxo_metrics.csv': 'BLOCKCHAIN_METRICS',  # Merge into blockchain
        'transaction_metrics.csv': 'BLOCKCHAIN_METRICS',  # Merge into blockchain
        'difficulty_adjustment.csv': 'MINING_METRICS',  # Merge into mining
    }
    
    print("Loading NewHedge data into Snowflake...")
    
    # Process files that can be directly loaded
    for csv_file, table_name in file_table_mapping.items():
        csv_path = os.path.join(NEWHEDGE_DIR, csv_file)
        
        if not os.path.exists(csv_path):
            print(f"  ⚠️  {csv_file} not found, skipping...")
            continue
        
        print(f"Processing {csv_file} -> {table_name}...")
        
        try:
            df = pd.read_csv(csv_path)
            
            if df.empty:
                print(f"  ⚠️  {csv_file} is empty, skipping...")
                continue
            
            # Normalize columns
            df.columns = [c.upper().replace(' ', '_').replace('(', '').replace(')', '').replace('-', '_') for c in df.columns]
            
            # Convert timestamp
            if 'TIMESTAMP' in df.columns:
                df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'])
            
            # Merge data
            merge_data_to_table(conn, df, table_name)
            
        except Exception as e:
            print(f"  ✗ Error processing {csv_file}: {e}")

def export_snowflake_to_csv(conn):
    """Export all NewHedge tables from Snowflake to CSV files."""
    
    print("\nExporting Snowflake tables to CSV...")
    
    cursor = conn.cursor()
    
    # Get list of tables in NEWHEDGE schema
    cursor.execute("SHOW TABLES IN SCHEMA NEWHEDGE")
    tables = [row[1] for row in cursor.fetchall()]
    
    export_dir = os.path.join(DATA_DIR, 'newhedge_export')
    os.makedirs(export_dir, exist_ok=True)
    
    for table_name in tables:
        try:
            print(f"Exporting NEWHEDGE.{table_name}...")
            
            # Query all data from table
            query = f"SELECT * FROM NEWHEDGE.{table_name} ORDER BY TIMESTAMP DESC"
            cursor.execute(query)
            
            # Fetch results
            df = cursor.fetch_pandas_all()
            
            if df.empty:
                print(f"  ⚠️  {table_name} is empty, skipping...")
                continue
            
            # Export to CSV
            output_file = os.path.join(export_dir, f"{table_name.lower()}.csv")
            df.to_csv(output_file, index=False)
            
            print(f"  ✓ Exported {len(df)} rows to {output_file}")
            
        except Exception as e:
            print(f"  ✗ Error exporting {table_name}: {e}")
    
    cursor.close()
    print(f"\n✓ All tables exported to {export_dir}")

def main():
    """Main execution function."""
    conn = get_snowflake_conn()
    
    if not conn:
        print("❌ Could not connect to Snowflake. Please check your credentials.")
        return
    
    print("✓ Connected to Snowflake")
    
    # Load data into Snowflake
    load_newhedge_data(conn)
    
    # Export data back to CSV
    export_snowflake_to_csv(conn)
    
    conn.close()
    print("\n✓ Process completed successfully!")

if __name__ == "__main__":
    main()
