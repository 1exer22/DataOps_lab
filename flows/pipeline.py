"""
Orchestrateur complet du pipeline DataOps — piloté par Prefect.

Prefect orchestre chaque etape de bout en bout :
  1. Ingestion  - MinIO + Postgres -> DuckDB staging  (sous-flow Prefect)
  2. Qualite    - Soda checks sur le staging           (task Prefect)
  3. Transform  - dbt run                              (task Prefect)
  4. Tests dbt  - dbt test                             (task Prefect)
  5. Rapport    - ecrit last_run.json                  (task Prefect)

Prerequis : serveur Prefect demarre
  prefect server start

Usage :
  python flows/pipeline.py
  python flows/pipeline.py --skip-ingest
"""

import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from prefect import flow, task, get_run_logger
from prefect.context import get_run_context

# flows/pipeline.py -> parent = flows/ -> parent = Tpass/
ROOT    = Path(__file__).parent.parent
DBT_DIR = ROOT / "dbt_project"
REPORT  = ROOT / "last_run.json"
PYTHON  = ROOT / "venv" / "Scripts" / "python.exe"
DBT     = ROOT / "venv" / "Scripts" / "dbt.exe"


# ── tasks Prefect ─────────────────────────────────────────────────────────────

@task(name="soda-checks-staging", retries=1, retry_delay_seconds=10)
def task_soda() -> bool:
    logger = get_run_logger()
    logger.info("Lancement des checks Soda sur le staging...")
    result = subprocess.run(
        [str(PYTHON), str(ROOT / "run_soda.py"), "--level", "1"],
        cwd=str(ROOT),
        capture_output=False,
    )
    if result.returncode != 0:
        raise Exception("Soda : echec des checks qualite — pipeline bloque.")
    logger.info("Soda : tous les checks passent.")
    return True


@task(name="dbt-run", retries=1, retry_delay_seconds=10)
def task_dbt_run() -> bool:
    logger = get_run_logger()
    logger.info("dbt run : construction des modeles...")
    result = subprocess.run(
        [str(DBT), "run"],
        cwd=str(DBT_DIR),
        capture_output=False,
    )
    if result.returncode != 0:
        raise Exception("dbt run : echec de la construction des modeles.")
    logger.info("dbt run : tous les modeles sont construits.")
    return True


@task(name="dbt-test")
def task_dbt_test() -> bool:
    logger = get_run_logger()
    logger.info("dbt test : verification de la qualite des marts...")
    result = subprocess.run(
        [str(DBT), "test"],
        cwd=str(DBT_DIR),
        capture_output=False,
    )
    if result.returncode != 0:
        logger.warning("dbt test : certains tests ont echoue — marts construits mais anomalies detectees.")
        return False
    logger.info("dbt test : tous les tests passent.")
    return True


@task(name="ecriture-rapport")
def task_write_report(steps: dict) -> None:
    logger = get_run_logger()
    report = {
        "last_run": datetime.now().isoformat(timespec="seconds"),
        "steps": steps,
        "status": "OK" if all(v in ("OK", "SKIP") for v in steps.values()) else "ECHEC",
    }
    REPORT.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info(f"Rapport ecrit : {REPORT}")


# ── flow principal ────────────────────────────────────────────────────────────

@flow(
    name="pipeline-dataops",
    description="Ingestion -> Qualite Soda -> dbt run -> dbt test",
)
def pipeline_flow(skip_ingest: bool = False):
    logger = get_run_logger()
    steps: dict[str, str] = {}

    # 1. Ingestion — sous-flow Prefect (ingestion_flow.py)
    if skip_ingest:
        logger.info("Ingestion ignoree (skip_ingest=True).")
        steps["ingestion"] = "SKIP"
    else:
        logger.info("Etape 1 : ingestion depuis MinIO et Postgres...")
        sys.path.insert(0, str(ROOT / "flows"))
        try:
            from ingestion_flow import ingestion_flow
            ingestion_flow()
            steps["ingestion"] = "OK"
        except Exception as e:
            steps["ingestion"] = "ECHEC"
            task_write_report(steps)
            raise RuntimeError(f"Ingestion echouee : {e}") from e
        finally:
            sys.path.pop(0)

    # 2. Soda checks — bloquant
    logger.info("Etape 2 : checks qualite Soda...")
    try:
        task_soda()
        steps["soda"] = "OK"
    except Exception as e:
        steps["soda"] = "ECHEC"
        task_write_report(steps)
        raise

    # 3. dbt run — bloquant
    logger.info("Etape 3 : transformations dbt...")
    try:
        task_dbt_run()
        steps["dbt_run"] = "OK"
    except Exception as e:
        steps["dbt_run"] = "ECHEC"
        task_write_report(steps)
        raise

    # 4. dbt test — non bloquant (warning seulement)
    logger.info("Etape 4 : tests dbt sur les marts...")
    dbt_ok = task_dbt_test()
    steps["dbt_test"] = "OK" if dbt_ok else "ECHEC"

    # 5. Rapport
    task_write_report(steps)

    logger.info(f"Pipeline termine : {steps}")
    return steps


# ── entrypoint ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--skip-ingest", action="store_true",
        help="Saute l'ingestion si les donnees sont deja en staging"
    )
    args = parser.parse_args()
    pipeline_flow(skip_ingest=args.skip_ingest)
