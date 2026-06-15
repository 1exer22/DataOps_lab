import os
os.environ["PREFECT_API_URL"] = "http://127.0.0.1:4200/api"
 
from prefect import flow, task
import pandas as pd
import duckdb
from sqlalchemy import create_engine
from minio import Minio
from io import BytesIO
 
 
# -------------------------
# CONFIG
# -------------------------
 
DUCKDB_PATH = "warehouse.duckdb"
 
POSTGRES_CONN = "postgresql://dataops:dataops@127.0.0.1:5433/oltp"
 
MINIO_CLIENT = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)
 
BUCKET = "datastore"
 
 
# -------------------------
# MINIO EXTRACT
# -------------------------
 
@task
def extract_orders_from_minio():
    obj = MINIO_CLIENT.get_object(BUCKET, "orders.csv")
    df = pd.read_csv(BytesIO(obj.read()))
    return df
 
 
@task
def extract_order_items_from_minio():
    obj = MINIO_CLIENT.get_object(BUCKET, "order_items.csv")
    df = pd.read_csv(BytesIO(obj.read()))
    return df
 
 
# -------------------------
# POSTGRES EXTRACT
# -------------------------
 
@task
def extract_customers():
    engine = create_engine(POSTGRES_CONN)
    df = pd.read_sql("SELECT * FROM customers", engine)
    return df
 
 
@task
def extract_products():
    engine = create_engine(POSTGRES_CONN)
    df = pd.read_sql("SELECT * FROM products", engine)
    return df
 
 
@task
def clean_dataframe(df):
    df = df.copy()
 
    # normalize column names
    df.columns = [c.strip().lower() for c in df.columns]
 
    # basic cleanup
    df = df.drop_duplicates()
 
    return df
 
# -------------------------
# DUCKDB LOAD
# -------------------------
 
@task
def load_to_duckdb(df, table_name):
    con = duckdb.connect(DUCKDB_PATH)
 
    # ensure schema exists
    con.execute("CREATE SCHEMA IF NOT EXISTS staging")
 
    # safe copy
    df = df.copy()
 
    # safer type conversion for DuckDB
    df = df.astype("string")
 
    # register dataframe
    con.register("tmp", df)
 
    # write to DuckDB
    con.execute(f"""
        CREATE OR REPLACE TABLE staging.{table_name} AS
        SELECT * FROM tmp
    """)
 
    # row count validation
    count = con.execute(f"""
        SELECT COUNT(*) FROM staging.{table_name}
    """).fetchone()[0]
 
    print(f"Loaded {table_name}: {count} rows")
 
    con.close()
 
@task
def validate_load(table_name):
    con = duckdb.connect(DUCKDB_PATH)
 
    count = con.execute(f"SELECT COUNT(*) FROM staging.{table_name}").fetchone()[0]
    sample = con.execute(f"SELECT * FROM staging.{table_name} LIMIT 5").df()
 
    con.close()
 
    print(f"\n📦 {table_name}: {count} rows")
    print(sample)
 
    return count
 
# -------------------------
# FLOW
# -------------------------
 
@flow(log_prints=True)
def ingestion_flow():
 
    # Extract
    orders = extract_orders_from_minio()
    order_items = extract_order_items_from_minio()
    customers = extract_customers()
    products = extract_products()
   
    # Clean
    orders = clean_dataframe(orders)
    order_items = clean_dataframe(order_items)
    customers = clean_dataframe(customers)
    products = clean_dataframe(products)
 
    # Load
    load_to_duckdb(orders, "orders")
    validate_load("orders")
    load_to_duckdb(order_items, "order_items")
    load_to_duckdb(customers, "customers")
    load_to_duckdb(products, "products")
 
    print("All sources loaded into DuckDB staging")
 
 
# -------------------------
# ENTRYPOINT
# -------------------------
 
if __name__ == "__main__":
    ingestion_flow()
 