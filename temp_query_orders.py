import psycopg2
from psycopg2.extras import RealDictCursor

CONN_STRING = "postgresql://tsdbadmin:kxv64cdo85nezfz7@de4acscajk.cdlvvyxvi0.db.ghost.build:5432/tsdb?sslmode=require"

with psycopg2.connect(CONN_STRING) as conn:
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute("SELECT * FROM staging.orders LIMIT 10;")
        rows = cursor.fetchall()

print('Top 10 rows from staging.orders:')
if not rows:
    print('No rows returned.')
else:
    headers = rows[0].keys()
    print(', '.join(headers))
    for row in rows:
        print(', '.join(str(row[h]) if row[h] is not None else 'NULL' for h in headers))
