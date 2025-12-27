import shioaji as sj
import pandas as pd
import psycopg2
from psycopg2 import sql
import os
import sys
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

# Shioaji API credentials
API_KEY = os.getenv('API_KEY')
SECRET_KEY = os.getenv('SECRET_KEY')

# PostgreSQL connection parameters from environment
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

def read_symbols(file_path='symbols.txt'):
    """Read symbols from text file."""
    with open(file_path, 'r') as f:
        symbols = [line.strip() for line in f if line.strip()]
    return symbols

def create_kbars_table(conn):
    """Create kbars table if it doesn't exist."""
    cur = conn.cursor()

    # Create table
    create_table_query = """
    CREATE TABLE IF NOT EXISTS stock_kbars (
        id SERIAL PRIMARY KEY,
        symbol VARCHAR(20) NOT NULL,
        time TIMESTAMP NOT NULL,
        open NUMERIC(10, 2),
        high NUMERIC(10, 2),
        low NUMERIC(10, 2),
        close NUMERIC(10, 2),
        volume BIGINT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(symbol, time)
    );
    """
    cur.execute(create_table_query)
    conn.commit()

    # Create indexes
    index_queries = [
        "CREATE INDEX IF NOT EXISTS idx_stock_kbars_symbol ON stock_kbars(symbol);",
        "CREATE INDEX IF NOT EXISTS idx_stock_kbars_time ON stock_kbars(time);",
        "CREATE INDEX IF NOT EXISTS idx_stock_kbars_symbol_time ON stock_kbars(symbol, time);"
    ]

    for index_query in index_queries:
        cur.execute(index_query)
        conn.commit()

    cur.close()
    print("✓ Table 'stock_kbars' created or already exists")

def insert_kbars_data(conn, symbol, df):
    """Insert kbar data into PostgreSQL."""
    if df.empty:
        return 0

    cur = conn.cursor()

    insert_query = """
    INSERT INTO stock_kbars (symbol, time, open, high, low, close, volume)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (symbol, time) DO UPDATE SET
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        volume = EXCLUDED.volume;
    """

    inserted_count = 0
    for _, row in df.iterrows():
        try:
            cur.execute(insert_query, (
                symbol,
                row['time'],
                float(row['Open']) if pd.notna(row['Open']) else None,
                float(row['High']) if pd.notna(row['High']) else None,
                float(row['Low']) if pd.notna(row['Low']) else None,
                float(row['Close']) if pd.notna(row['Close']) else None,
                int(row['Volume']) if pd.notna(row['Volume']) else None
            ))
            inserted_count += 1
        except Exception as e:
            print(f"  Error inserting row for {symbol}: {e}")
            continue

    conn.commit()
    cur.close()
    return inserted_count

def fetch_and_insert_kbars(start_date=None, end_date=None):
    """Main function to fetch kbars and insert into database."""

    # Default to today if no dates provided
    if start_date is None:
        target_date = datetime.now()
        start_date = target_date
        end_date = target_date  # Get just one day (today)
    else:
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
        if end_date is None:
            end_date = start_date  # If only start provided, get just that day
        else:
            end_date = datetime.strptime(end_date, "%Y-%m-%d")

    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    print(f"Fetching kbars from {start_str} to {end_str}")

    # Read symbols
    symbols = read_symbols()
    print(f"\n✓ Loaded {len(symbols)} symbols from symbols.txt")

    # Connect to Shioaji
    print("\nConnecting to Shioaji API...")
    api = sj.Shioaji(simulation=False)
    api.login(api_key=API_KEY, secret_key=SECRET_KEY)
    print("✓ Connected to Shioaji API")

    # Wait for contracts to load
    time.sleep(2)

    # Connect to PostgreSQL
    print("\nConnecting to PostgreSQL...")
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    print("✓ Connected to PostgreSQL")

    # Create table
    create_kbars_table(conn)

    # Fetch and insert data for each symbol
    print(f"\nFetching kbars for {len(symbols)} symbols...")
    total_rows = 0
    successful_symbols = 0
    failed_symbols = []

    for i, symbol in enumerate(symbols, 1):
        try:
            print(f"\n[{i}/{len(symbols)}] Processing {symbol}...")

            # Get contract
            contract = api.Contracts.Stocks[symbol]

            # Fetch kbars
            kbars = api.kbars(
                contract=contract,
                start=start_str,
                end=end_str
            )

            # Convert to DataFrame
            df = pd.DataFrame({**kbars})

            if df.empty:
                print(f"  ⚠ No data returned for {symbol}")
                continue

            # Convert timestamp - Shioaji returns Unix timestamps that represent Taiwan local time
            # pd.to_datetime interprets them as UTC, so we need to shift back 8 hours
            df['time'] = pd.to_datetime(df['ts']) - pd.Timedelta(hours=8)
            df = df.drop(columns=['ts'])  # Remove the old 'ts' column

            print(f"  Retrieved {len(df)} bars")

            # Insert into database
            inserted = insert_kbars_data(conn, symbol, df)
            total_rows += inserted
            successful_symbols += 1
            print(f"  ✓ Inserted {inserted} rows")

            # Rate limiting - be nice to the API
            time.sleep(1)

        except KeyError:
            print(f"  ✗ Symbol {symbol} not found in contracts")
            failed_symbols.append(symbol)
        except Exception as e:
            print(f"  ✗ Error processing {symbol}: {e}")
            failed_symbols.append(symbol)
            continue

    # Close connections
    conn.close()
    api.logout()

    # Summary
    print("\n" + "="*60)
    print("Summary")
    print("="*60)
    print(f"Total symbols processed: {len(symbols)}")
    print(f"Successful: {successful_symbols}")
    print(f"Failed: {len(failed_symbols)}")
    if failed_symbols:
        print(f"Failed symbols: {', '.join(failed_symbols)}")
    print(f"Total rows inserted/updated: {total_rows}")
    print("="*60)

if __name__ == "__main__":
    # Parse command line arguments
    # Usage:
    #   python fetch_and_insert_kbars.py              # Fetch today's data
    #   python fetch_and_insert_kbars.py 2025-12-26   # Fetch specific date
    #   python fetch_and_insert_kbars.py 2025-01-01 2025-12-27  # Fetch date range

    if len(sys.argv) == 1:
        # No arguments - fetch today
        fetch_and_insert_kbars()
    elif len(sys.argv) == 2:
        # One argument - fetch specific date
        fetch_and_insert_kbars(sys.argv[1])
    elif len(sys.argv) == 3:
        # Two arguments - fetch date range
        fetch_and_insert_kbars(sys.argv[1], sys.argv[2])
    else:
        print("Usage: python fetch_and_insert_kbars.py [start_date] [end_date]")
        print("Examples:")
        print("  python fetch_and_insert_kbars.py              # Fetch today")
        print("  python fetch_and_insert_kbars.py 2025-12-26   # Fetch specific date")
        print("  python fetch_and_insert_kbars.py 2025-01-01 2025-12-27  # Fetch range")
        sys.exit(1)