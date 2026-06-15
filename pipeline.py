"""
Orchestrateur complet du pipeline DataOps.

Etapes :
  1. Ingestion  — MinIO + Postgres -> DuckDB staging  (ingestion_flow.py)
  2. Qualite    — Soda checks sur le staging           (run_soda.py niveau 1)
  3. Transform  — dbt run                              (models staging/intermediate/marts)
  4. Tests dbt  — dbt test                             (schema.yml)
  5. Rapport    — ecrit last_run.json pour Streamlit

Usage :
  python pipeline.py              # pipeline complet
  python pipeline.py --skip-ingest  # saute l'ingestion (donnees deja en staging)
"""

import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

ROOT    = Path(__file__).parent
DBT_DIR = ROOT / "dbt_project"
REPORT  = ROOT / "last_run.json"
PYTHON  = ROOT / "venv" / "Scripts" / "python.exe"
DBT     = ROOT / "venv" / "Scripts" / "dbt.exe"

GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
BOLD   = "\033[1m"
RESET  = "\033[0m"


# ── utilitaires ───────────────────────────────────────────────────────────────

def banner(title: str):
    print(f"\n{BOLD}{'='*60}{RESET}")
    print(f"{BOLD}  {title}{RESET}")
    print(f"{BOLD}{'='*60}{RESET}")


def run(cmd: list, cwd: Path = ROOT, label: str = "") -> int:
    print(f"\n  $ {' '.join(str(c) for c in cmd)}")
    result = subprocess.run(cmd, cwd=str(cwd))
    return result.returncode


def write_report(steps: dict):
    report = {
        "last_run": datetime.now().isoformat(timespec="seconds"),
        "steps": steps,
        "status": "OK" if all(v == "OK" for v in steps.values()) else "ECHEC",
    }
    REPORT.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\n  Rapport ecrit -> {REPORT.name}")


# ── etapes ────────────────────────────────────────────────────────────────────

def step_ingestion() -> str:
    banner("ETAPE 1 — Ingestion (MinIO + Postgres -> DuckDB staging)")
    code = run([str(PYTHON), str(ROOT / "flows" / "ingestion_flow.py")])
    return "OK" if code == 0 else "ECHEC"


def step_soda() -> str:
    banner("ETAPE 2 — Qualite des donnees (Soda checks staging)")
    code = run([str(PYTHON), str(ROOT / "run_soda.py"), "--level", "1"])
    return "OK" if code == 0 else "ECHEC"


def step_dbt_run() -> str:
    banner("ETAPE 3 — Transformations (dbt run)")
    code = run([str(DBT), "run"], cwd=DBT_DIR)
    return "OK" if code == 0 else "ECHEC"


def step_dbt_test() -> str:
    banner("ETAPE 4 — Tests qualite marts (dbt test)")
    code = run([str(DBT), "test"], cwd=DBT_DIR)
    return "OK" if code == 0 else "ECHEC"


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-ingest", action="store_true",
                        help="Saute l'etape d'ingestion (donnees deja en staging)")
    args = parser.parse_args()

    print(f"\n{BOLD}PIPELINE DATAOPS — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}{RESET}")
    steps: dict[str, str] = {}

    # 1. Ingestion
    if args.skip_ingest:
        print(f"\n{YELLOW}  >> Ingestion ignoree (--skip-ingest){RESET}")
        steps["ingestion"] = "SKIP"
    else:
        steps["ingestion"] = step_ingestion()
        if steps["ingestion"] == "ECHEC":
            print(f"\n{RED}{BOLD}  Arret : echec de l ingestion.{RESET}")
            write_report(steps)
            sys.exit(1)

    # 2. Soda checks — bloquant si FAIL
    steps["soda"] = step_soda()
    if steps["soda"] == "ECHEC":
        print(f"\n{RED}{BOLD}  Arret : checks qualite en echec.{RESET}")
        print(f"  Les donnees en staging ne respectent pas le contrat de schema.")
        print(f"  Corrigez le fichier source et relancez.")
        write_report(steps)
        sys.exit(1)

    # 3. dbt run
    steps["dbt_run"] = step_dbt_run()
    if steps["dbt_run"] == "ECHEC":
        print(f"\n{RED}{BOLD}  Arret : dbt run en echec.{RESET}")
        write_report(steps)
        sys.exit(1)

    # 4. dbt test
    steps["dbt_test"] = step_dbt_test()
    if steps["dbt_test"] == "ECHEC":
        print(f"\n{YELLOW}{BOLD}  Avertissement : certains tests dbt ont echoue.{RESET}")
        print(f"  Les marts sont construits mais des anomalies ont ete detectees.")

    # 5. Rapport final
    write_report(steps)

    banner("PIPELINE TERMINE")
    for name, status in steps.items():
        color = GREEN if status in ("OK", "SKIP") else (YELLOW if status == "ECHEC" and name == "dbt_test" else RED)
        print(f"  {color}{status:6}{RESET}  {name}")

    overall_ok = all(v in ("OK", "SKIP") for v in steps.values())
    print(f"\n  {GREEN if overall_ok else RED}{BOLD}{'OK' if overall_ok else 'ECHEC'}{RESET}\n")
    sys.exit(0 if overall_ok else 1)


if __name__ == "__main__":
    main()
