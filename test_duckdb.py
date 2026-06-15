import duckdb

con = duckdb.connect('warehouse.duckdb')

con.execute("CREATE SCHEMA IF NOT EXISTS staging")

print(con.execute("SELECT schema_name FROM information_schema.schemata").fetchall())

con.close()

print("DuckDB OK")