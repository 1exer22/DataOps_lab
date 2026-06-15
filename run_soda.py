"""
Runner de checks qualite compatible avec la syntaxe des fichiers Soda YAML.
Utilise DuckDB directement (pas de conflit de version avec soda-core-duckdb).

Usage :
    python run_soda.py                    # niveau 1 : source -> staging
    python run_soda.py --level 1          # idem
    python run_soda.py --level 2          # niveau 2 : dbt test (appel externe)
    python run_soda.py --all              # les deux niveaux
"""

import argparse
import subprocess
import sys
from pathlib import Path

import duckdb
import yaml

DB_PATH   = Path(__file__).parent / "warehouse.duckdb"
SODA_DIR  = Path(__file__).parent / "soda"
DBT_DIR   = Path(__file__).parent / "dbt_project"
CHECKS_L1 = SODA_DIR / "checks_source_to_staging.yml"


# ── helpers de formatage ──────────────────────────────────────────────────────

GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
RESET  = "\033[0m"
BOLD   = "\033[1m"

def ok(msg):   print(f"  {GREEN}PASS{RESET}  {msg}")
def fail(msg): print(f"  {RED}FAIL{RESET}  {msg}")
def warn(msg): print(f"  {YELLOW}WARN{RESET}  {msg}")
def header(msg): print(f"\n{BOLD}{msg}{RESET}")


# ── logique d'execution des checks ───────────────────────────────────────────

def get_columns(con, schema_table: str) -> list[str]:
    schema, table = schema_table.split(".")
    rows = con.execute(
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_schema='{schema}' AND table_name='{table}'"
    ).fetchall()
    return [r[0] for r in rows]


def run_check(con, table: str, check: dict) -> tuple[int, int, int, bool]:
    """Retourne (passed, failed, warned, schema_ok)."""
    passed = failed = warned = 0
    schema_ok = True

    for key, value in check.items():

        # --- schema : colonnes manquantes ---
        if key == "schema":
            name = value.get("name", "schema check")
            missing_cols = value.get("fail", {}).get("when missing column", [])
            existing = get_columns(con, table)
            missing = [c for c in missing_cols if c not in existing]
            if missing:
                fail(f"{name} — colonnes manquantes : {missing}")
                failed += 1
                schema_ok = False
            else:
                ok(f"{name}")
                passed += 1

        # --- row_count ---
        elif key.startswith("row_count"):
            name = check.get("name", key)
            count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            # parse "row_count > 0" ou "row_count = 4"
            op   = key.split(" ")[1] if " " in key else ">"
            val  = int(key.split(" ")[2]) if " " in key else 0
            bounds = value.get("fail", {}).get("when not between") if isinstance(value, dict) else None
            if bounds:
                lo, hi = bounds
                if lo <= count <= hi:
                    ok(f"{name} ({count} lignes)")
                    passed += 1
                else:
                    fail(f"{name} ({count} lignes, attendu entre {lo} et {hi})")
                    failed += 1
            else:
                result = eval(f"{count} {op} {val}")
                if result:
                    ok(f"{name} ({count} lignes)")
                    passed += 1
                else:
                    fail(f"{name} ({count} lignes)")
                    failed += 1

        # --- missing_count(col) = 0 ---
        elif key.startswith("missing_count"):
            col  = key[key.index("(")+1 : key.index(")")]
            name = check.get("name", f"missing_count({col})")
            cnt  = con.execute(
                f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL"
            ).fetchone()[0]
            expected = int(key.split("=")[1].strip()) if "=" in key else 0
            if cnt == expected:
                ok(f"{name} (0 nul)")
                passed += 1
            else:
                fail(f"{name} ({cnt} valeurs nulles)")
                failed += 1

        # --- duplicate_count(col) = 0 ---
        elif key.startswith("duplicate_count"):
            col  = key[key.index("(")+1 : key.index(")")]
            name = check.get("name", f"duplicate_count({col})")
            cnt  = con.execute(
                f"SELECT COUNT(*) FROM ("
                f"  SELECT {col} FROM {table} GROUP BY {col} HAVING COUNT(*) > 1"
                f") t"
            ).fetchone()[0]
            if cnt == 0:
                ok(f"{name} (pas de doublon)")
                passed += 1
            else:
                fail(f"{name} ({cnt} valeur(s) en doublon)")
                failed += 1

        # --- invalid_count(col) = 0 ---
        elif key.startswith("invalid_count"):
            col  = key[key.index("(")+1 : key.index(")")]
            name = check.get("name", f"invalid_count({col})")
            valid_vals = value.get("valid values", []) if isinstance(value, dict) else []
            if not valid_vals:
                continue
            placeholders = ", ".join(f"'{v}'" for v in valid_vals)
            cnt = con.execute(
                f"SELECT COUNT(*) FROM {table} "
                f"WHERE {col} IS NOT NULL AND {col} NOT IN ({placeholders})"
            ).fetchone()[0]
            if cnt == 0:
                ok(f"{name}")
                passed += 1
            else:
                bad = con.execute(
                    f"SELECT DISTINCT {col} FROM {table} "
                    f"WHERE {col} NOT IN ({placeholders}) LIMIT 5"
                ).fetchall()
                fail(f"{name} ({cnt} valeur(s) invalide(s) : {[r[0] for r in bad]})")
                failed += 1

        # --- min(col) avec fail/warn ---
        elif key.startswith("min("):
            col  = key[key.index("(")+1 : key.index(")")]
            name = check.get("name", f"min({col})")
            val  = con.execute(f"SELECT MIN(TRY_CAST({col} AS DOUBLE)) FROM {table}").fetchone()[0]
            if val is None:
                warn(f"{name} (colonne vide)")
                warned += 1
                continue
            fail_cfg = value.get("fail", {}) if isinstance(value, dict) else {}
            warn_cfg = value.get("warn", {}) if isinstance(value, dict) else {}
            threshold_fail = fail_cfg.get("when_lt") if isinstance(fail_cfg, dict) else None
            threshold_warn = warn_cfg.get("when_lt") if isinstance(warn_cfg, dict) else None
            if threshold_fail is not None and val < threshold_fail:
                fail(f"{name} (min={val}, seuil fail < {threshold_fail})")
                failed += 1
            elif threshold_warn is not None and val < threshold_warn:
                warn(f"{name} (min={val}, seuil warn < {threshold_warn})")
                warned += 1
            else:
                ok(f"{name} (min={val})")
                passed += 1

    return passed, failed, warned, schema_ok


def run_level1() -> int:
    header("=" * 60)
    header("NIVEAU 1 — Source -> DuckDB Staging (Soda checks)")
    header("=" * 60)

    with open(CHECKS_L1, encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    # Le YAML Soda utilise des cles "checks for <table>:"
    # PyYAML les parse comme cles dict normales
    total_p = total_f = total_w = 0
    con = duckdb.connect(str(DB_PATH), read_only=True)

    for key, checks in raw.items():
        if not key.startswith("checks for "):
            continue
        table = key.replace("checks for ", "").strip()
        header(f"\n  Table : {table}")

        schema_valid = True
        for check_item in checks:
            if not isinstance(check_item, dict):
                continue
            if not schema_valid and "schema" not in check_item:
                warn("  check ignore (schema invalide)")
                total_w += 1
                continue
            p, f_, w, s_ok = run_check(con, table, check_item)
            total_p += p
            total_f += f_
            total_w += w
            if not s_ok:
                schema_valid = False

    con.close()
    _print_summary(total_p, total_f, total_w)
    return 1 if total_f > 0 else 0


def run_level2() -> int:
    header("=" * 60)
    header("NIVEAU 2 — Marts (dbt test)")
    header("=" * 60)

    venv_dbt = DBT_DIR.parent / "venv" / "Scripts" / "dbt.exe"
    result = subprocess.run(
        [str(venv_dbt), "test"],
        cwd=str(DBT_DIR),
        capture_output=False,
    )
    return result.returncode


def _print_summary(passed: int, failed: int, warned: int):
    total = passed + failed + warned
    print(f"\n{BOLD}  Resultats : {passed}/{total} OK — {failed} FAIL — {warned} WARN{RESET}")


def main():
    parser = argparse.ArgumentParser()
    grp = parser.add_mutually_exclusive_group()
    grp.add_argument("--level", type=int, choices=[1, 2])
    grp.add_argument("--all", action="store_true")
    args = parser.parse_args()

    codes = []
    if args.level == 2:
        codes.append(run_level2())
    elif args.all:
        codes.append(run_level1())
        codes.append(run_level2())
    else:
        codes.append(run_level1())

    overall = max(codes)
    print(f"\n{'='*60}")
    status = f"{GREEN}OK{RESET}" if overall == 0 else f"{RED}ECHEC{RESET}"
    print(f"{BOLD}RESULTAT GLOBAL : {status}{RESET}")
    print("=" * 60)
    sys.exit(overall)


if __name__ == "__main__":
    main()
