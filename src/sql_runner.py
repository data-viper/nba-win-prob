from pathlib import Path
import duckdb

PARQUET_PATH = "data/games_seasons_2023_2024.parquet"
SQL_FILE = "analysis/eda.sql"

def main():
    con = duckdb.connect()

    # Expose parquet as a SQL view named "games"
    con.execute(f"""
    CREATE OR REPLACE VIEW games AS
    SELECT * FROM read_parquet('{PARQUET_PATH}');
    """)

    sql_text = Path(SQL_FILE).read_text(encoding="utf-8").strip()
    if not sql_text:
        print("analysis/eda.sql is empty.")
        return

    # Split on semicolons so you can write multiple queries in one file
    statements = [s.strip() for s in sql_text.split(";") if s.strip()]

    for i, stmt in enumerate(statements, start=1):
        # Skip comment-only blocks
        if stmt.startswith("--"):
            # keep it simple: still run if query exists after comments
            pass

        print(f"\n====================")
        print(f"Query {i}")
        print(f"====================")
        print(stmt)

        try:
            df = con.execute(stmt).fetchdf()
            print("\nResult:")
            print(df)
        except Exception as e:
            print(f"\n❌ Error in Query {i}: {e}")
            break

if __name__ == "__main__":
    main()
