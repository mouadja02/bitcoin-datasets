import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

load_dotenv()

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')

def get_snowflake_conn():
    try:
        conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA')
        )
        return conn
    except Exception as e:
        print(f"Could not connect to Snowflake: {e}")
        return None

def upload_folder(conn, folder_name, schema_name='PUBLIC'):
    """Upload CSV files from a folder to Snowflake with schema support."""
    folder_path = os.path.join(DATA_DIR, folder_name)
    if not os.path.exists(folder_path):
        return

    # Set the schema
    cursor = conn.cursor()
    cursor.execute(f"USE SCHEMA {schema_name}")
    cursor.close()

    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith(".csv"):
                file_path = os.path.join(root, file)
                print(f"Processing {file_path}...")
                
                try:
                    df = pd.read_csv(file_path)
                    
                    # sanitize columns
                    df.columns = [c.upper().replace(' ', '_').replace('(', '').replace(')', '').replace('-', '_') for c in df.columns]
                    
                    # Table name based on file name
                    # For CoinDesk: use the existing table names from migration
                    table_name = file.replace('.csv', '').upper()
                    
                    # Write to snowflake
                    success, n_chunks, n_rows, _ = write_pandas(
                        conn,
                        df,
                        table_name,
                        auto_create_table=False,  # Tables created by migration
                        quote_identifiers=False
                    )
                    
                    if success:
                        print(f"Uploaded {n_rows} rows to {schema_name}.{table_name}")
                    else:
                        print(f"Failed to upload {file} to {schema_name}.{table_name}")
                        
                except Exception as e:
                    print(f"Error uploading {file}: {e}")

def main():
    conn = get_snowflake_conn()
    if not conn:
        print("Skipping Snowflake update (no connection).")
        return

    # Upload CoinDesk data to COINDESK schema
    print("Uploading CoinDesk data...")
    upload_folder(conn, 'coindesk', 'COINDESK')
    
    # Upload NewHedge data to NEWHEDGE schema
    print("\nUploading NewHedge data...")
    upload_folder(conn, 'newhedge', 'NEWHEDGE')
    
    conn.close()
    print("\n✓ Upload completed!")


if __name__ == "__main__":
    main()
