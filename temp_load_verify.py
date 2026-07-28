import os
import pathlib
import csv
import psycopg2
from psycopg2 import sql

CONN_STRING = "postgresql://tsdbadmin:kxv64cdo85nezfz7@de4acscajk.cdlvvyxvi0.db.ghost.build:5432/tsdb?sslmode=require"
BASE_DIR = pathlib.Path("E:/Walmart_Project/walmart_dataset/data")
CSV_TO_TABLE = {
    "customers.csv": "staging.customers",
    "stores.csv": "staging.stores",
    "products.csv": "staging.products",
    "employees.csv": "staging.employees",
    "orders.csv": "staging.orders",
    "order_items.csv": "staging.order_items",
}

errors = []
results = []

with psycopg2.connect(CONN_STRING) as conn:
    with conn.cursor() as cursor:
        for csv_name, table_name in CSV_TO_TABLE.items():
            csv_path = BASE_DIR / csv_name
            if not csv_path.exists():
                errors.append(f"Missing file: {csv_path}")
                continue

            try:
                with open(csv_path, "r", newline="", encoding="utf-8") as f:
                    row_count = sum(1 for _ in csv.reader(f)) - 1
                    f.seek(0)
                    cursor.copy_expert(sql.SQL("COPY {} FROM STDIN WITH (FORMAT CSV, HEADER TRUE)").format(sql.SQL(table_name)), f)
                    conn.commit()

                cursor.execute(sql.SQL("SELECT COUNT(*) FROM {};").format(sql.SQL(table_name)))
                loaded_count = cursor.fetchone()[0]
                success = loaded_count == row_count
                results.append((table_name, csv_name, row_count, loaded_count, success))
            except Exception as exc:
                conn.rollback()
                errors.append(f"Failed loading {csv_name} into {table_name}: {exc}")

print("CSV import summary:\n")
for table_name, csv_name, expected, loaded, success in results:
    status = "OK" if success else "MISMATCH"
    print(f"{csv_name} -> {table_name}: expected={expected}, loaded={loaded}, status={status}")

if errors:
    print("\nErrors detected:")
    for err in errors:
        print(f"- {err}")
    raise SystemExit(1)

print("\nAll CSV files loaded and verified successfully.")
