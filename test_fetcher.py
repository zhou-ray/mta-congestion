from src.query import get_connection

conn = get_connection()

result = conn.execute("""
    SELECT 
        year,
        month,
        COUNT(*) as row_count,
        MIN(transit_timestamp) as first_record,
        MAX(transit_timestamp) as last_record
    FROM ridership
    GROUP BY year, month
    ORDER BY year, month
""").df()

print(f"Total months: {len(result)}")
print(f"Total rows: {result['row_count'].sum():,}")
print(result.to_string())