"""
Simule l'arrivee d'un fichier customers avec de mauvaises colonnes.

Mode defaut  : corrompt stg_customers (renomme les colonnes)
Mode restore : python demo_bad_file.py --restore
"""

import sys
import duckdb

DB  = "warehouse.duckdb"
# Definition originale generee par dbt
ORIGINAL_VIEW = "SELECT * FROM staging.customers"

con = duckdb.connect(DB)

if "--restore" in sys.argv:
    print(">>> Restauration de la vue originale stg_customers...")
    con.execute(f"CREATE OR REPLACE VIEW main_staging.stg_customers AS {ORIGINAL_VIEW}")
    con.execute("DROP TABLE IF EXISTS main_staging._stg_customers_backup")
    print(">>> Restauration OK.")
    print("    Verification : python run_soda.py")
else:
    print(">>> Sauvegarde des donnees originales...")
    con.execute("""
        CREATE OR REPLACE TABLE main_staging._stg_customers_backup AS
        SELECT customer_id, country, signup_date, segment
        FROM staging.customers
    """)
    print(">>> Corruption : renommage des colonnes...")
    print("    Avant : customer_id, country, signup_date, segment")
    print("    Apres : id, region, created_at, tier")
    con.execute("""
        CREATE OR REPLACE VIEW main_staging.stg_customers AS
        SELECT
            customer_id AS id,
            country     AS region,
            signup_date AS created_at,
            segment     AS tier
        FROM main_staging._stg_customers_backup
    """)
    print(">>> Corruption OK.")
    print("    Lance : python run_soda.py")
    print("    Puis  : python demo_bad_file.py --restore")

con.close()
