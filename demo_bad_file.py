"""
Simule l'arrivee d'un fichier customers avec de mauvaises colonnes.

Mode defaut  : corrompt stg_customers (renomme les colonnes)
Mode restore : python demo_bad_file.py --restore
"""

import sys
import duckdb

DB = "warehouse.duckdb"
con = duckdb.connect(DB)

if "--restore" in sys.argv:
    print(">>> Restauration depuis la sauvegarde...")
    con.execute("""
        CREATE OR REPLACE VIEW main_staging.stg_customers AS
        SELECT * FROM main_staging._stg_customers_backup
    """)
    con.execute("DROP TABLE IF EXISTS main_staging._stg_customers_backup")
    print(">>> Restauration OK. Lance : python run_soda.py pour verifier.")
else:
    print(">>> Sauvegarde de stg_customers originale...")
    con.execute("""
        CREATE OR REPLACE TABLE main_staging._stg_customers_backup
        AS SELECT * FROM main_staging.stg_customers
    """)
    print(">>> Remplacement par un fichier avec de mauvaises colonnes...")
    print("    Avant : customer_id, country, signup_date, segment")
    print("    Apres : id, region, created_at, tier")
    con.execute("""
        CREATE OR REPLACE VIEW main_staging.stg_customers AS
        SELECT
            customer_id  AS id,
            country      AS region,
            signup_date  AS created_at,
            segment      AS tier
        FROM main_staging._stg_customers_backup
    """)
    print(">>> Corruption OK. Lance : python run_soda.py")
    print("    Puis : python demo_bad_file.py --restore")

con.close()
