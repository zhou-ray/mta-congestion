import duckdb
import pandas

conn = duckdb.connect()

result = conn.execute("""
    SELECT 
        borough,
        SUM(ridership) as total_ridership,
        COUNT(*) as records
    FROM read_parquet('data/raw/**/*.parquet', hive_partitioning=true)
    GROUP BY borough
    ORDER BY total_ridership DESC
""").df()

print(result)