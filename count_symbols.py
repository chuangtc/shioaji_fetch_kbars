import psycopg2
from psycopg2 import sql
import os
from dotenv import load_dotenv

load_dotenv()

# Database connection parameters from environment
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

def find_last_kbar_date():
    """Connect to PostgreSQL database and find the last date of kbars."""
    try:
        # Establish connection
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )

        cur = conn.cursor()

        # First, get all table names
        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_type = 'BASE TABLE'
        """)

        tables = cur.fetchall()
        print(f"\nFound {len(tables)} tables in the database:")
        for table in tables:
            print(f"  - {table[0]}")

        # Find kbar-related tables
        print("\n" + "="*60)
        print("Analyzing kbar tables...")
        print("="*60)

        kbar_tables = [t[0] for t in tables if 'kbar' in t[0].lower()]

        if not kbar_tables:
            print("No kbar tables found. Checking all tables for date/time columns...")
            kbar_tables = [t[0] for t in tables]

        for table_name in kbar_tables:
            print(f"\n{'='*60}")
            print(f"Table: {table_name}")
            print('='*60)

            # Get all columns for this table
            cur.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = %s
                ORDER BY ordinal_position
            """, (table_name,))

            columns = cur.fetchall()
            print(f"Columns: {', '.join([f'{c[0]} ({c[1]})' for c in columns])}")

            # Look for date/time columns
            date_columns = [col[0] for col in columns if any(x in col[1].lower() for x in ['date', 'time', 'timestamp'])]

            if not date_columns:
                # Also check column names
                date_columns = [col[0] for col in columns if any(x in col[0].lower() for x in ['date', 'time', 'ts', 'dt'])]

            print(f"Date/Time columns: {', '.join(date_columns) if date_columns else 'None found'}")

            # Get row count
            query = sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table_name))
            cur.execute(query)
            row_count = cur.fetchone()[0]
            print(f"Total rows: {row_count:,}")

            # Check for symbol column
            symbol_columns = [col[0] for col in columns if 'symbol' in col[0].lower()]
            if symbol_columns:
                for sym_col in symbol_columns:
                    query = sql.SQL("SELECT COUNT(DISTINCT {}) FROM {}").format(
                        sql.Identifier(sym_col),
                        sql.Identifier(table_name)
                    )
                    cur.execute(query)
                    distinct_count = cur.fetchone()[0]
                    print(f"Distinct {sym_col}: {distinct_count}")

            # Find the last date for each date/time column
            if date_columns:
                print("\nLast dates:")
                for date_col in date_columns:
                    query = sql.SQL("SELECT MAX({}) FROM {}").format(
                        sql.Identifier(date_col),
                        sql.Identifier(table_name)
                    )
                    cur.execute(query)
                    max_date = cur.fetchone()[0]
                    print(f"  - MAX({date_col}): {max_date}")

                    # Also get min date
                    query = sql.SQL("SELECT MIN({}) FROM {}").format(
                        sql.Identifier(date_col),
                        sql.Identifier(table_name)
                    )
                    cur.execute(query)
                    min_date = cur.fetchone()[0]
                    print(f"  - MIN({date_col}): {min_date}")

                # Show sample of latest records
                if date_columns:
                    main_date_col = date_columns[0]
                    query = sql.SQL("SELECT * FROM {} ORDER BY {} DESC LIMIT 5").format(
                        sql.Identifier(table_name),
                        sql.Identifier(main_date_col)
                    )
                    cur.execute(query)
                    samples = cur.fetchall()

                    if samples:
                        print(f"\nLatest 5 records:")
                        col_names = [desc[0] for desc in cur.description]
                        for sample in samples:
                            print(f"  {dict(zip(col_names, sample))}")

        cur.close()
        conn.close()

        print("\n" + "="*60)
        print("Query completed successfully!")
        print("="*60)

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    find_last_kbar_date()
