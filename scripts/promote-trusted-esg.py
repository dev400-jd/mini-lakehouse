#!/usr/bin/env python3
"""
ESG Quality Gate: Promotion-Steuerung von Curated nach Trusted.

Phase 1: dbt run --select curated         (Curated-Refresh)
Phase 2: Great Expectations Checkpoint    (Quality Gate gegen curated_esg_emissions)
Phase 3: dbt run --select trusted_...     (Promotion in Trusted, nur bei gruenem Gate)

Exit-Codes:
  0 = Promotion erfolgreich (Trusted aktualisiert)
  1 = Gate blockiert (rote Expectation, Trusted unveraendert)
  2 = Technischer Fehler (dbt-Fehlschlag, GE-Connection-Problem etc.)

Aufruf:
  uv run python scripts/promote-trusted-esg.py
  uv run python scripts/promote-trusted-esg.py --skip-curated-refresh

Hinweis: dbt laeuft in dieser Sandbox NICHT lokal, sondern im
jupyter-Container. Phase 1 und 3 rufen daher
'docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt ..."'
auf. GE laeuft lokal in der uv-Umgebung.
"""

# Encoding-Sicherheit fuer Windows (cp1252 stdout): muss VOR allem anderen passieren.
import io
import sys

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", line_buffering=True)

import argparse
import os
import subprocess
from pathlib import Path

import great_expectations as gx

REPO_ROOT = Path(__file__).resolve().parent.parent
GE_DIR = REPO_ROOT / "great_expectations"
CHECKPOINT_NAME = "curated_esg_checkpoint"

# dbt laeuft im jupyter-Container — repo-Pfad dort: /home/jovyan/dbt
DBT_CONTAINER = "jupyter"
DBT_REMOTE_DIR = "/home/jovyan/dbt"


def print_phase_header(phase_num: int, title: str) -> None:
    print()
    print("=" * 70)
    print(f"  PHASE {phase_num}: {title}")
    print("=" * 70)
    print()


def run_dbt_command(args: list[str]) -> int:
    """
    Rufe dbt im jupyter-Container auf.
    Gibt Exit-Code zurueck (0 = erfolgreich, sonst dbt-spezifischer Fehler).
    """
    inner = f"cd {DBT_REMOTE_DIR} && dbt " + " ".join(args)
    cmd = ["docker", "compose", "exec", "-T", DBT_CONTAINER, "bash", "-lc", inner]
    print(f"  Aufruf: dbt {' '.join(args)}")
    print(f"  Container: {DBT_CONTAINER}, working dir: {DBT_REMOTE_DIR}")

    env = os.environ.copy()
    env.setdefault("MSYS_NO_PATHCONV", "1")
    result = subprocess.run(cmd, cwd=REPO_ROOT, env=env)
    return result.returncode


def run_ge_checkpoint() -> tuple[bool, dict]:
    """
    Rufe den GE-Checkpoint auf, gib (success, statistics) zurueck.
    """
    print(f"  GE Context: {GE_DIR}")
    print(f"  Checkpoint: {CHECKPOINT_NAME}")

    context = gx.get_context(context_root_dir=str(GE_DIR))
    result = context.run_checkpoint(checkpoint_name=CHECKPOINT_NAME)

    success = bool(result["success"])
    stats = {"evaluated": 0, "successful": 0, "unsuccessful": 0}
    for run_result in result["run_results"].values():
        s = run_result["validation_result"]["statistics"]
        stats["evaluated"] += s["evaluated_expectations"]
        stats["successful"] += s["successful_expectations"]
        stats["unsuccessful"] += s["unsuccessful_expectations"]
    return success, stats


def main() -> int:
    parser = argparse.ArgumentParser(description="ESG Quality Gate orchestrator")
    parser.add_argument(
        "--skip-curated-refresh",
        action="store_true",
        help="Phase 1 ueberspringen (z.B. nach manueller Curated-Manipulation fuer Demo)",
    )
    args = parser.parse_args()

    # Phase 1
    if args.skip_curated_refresh:
        print()
        print("(Phase 1 uebersprungen via --skip-curated-refresh)")
    else:
        print_phase_header(1, "Curated-Refresh via dbt")
        rc = run_dbt_command(["run", "--select", "curated"])
        if rc != 0:
            print()
            print("FEHLER: dbt run fuer Curated fehlgeschlagen.")
            print("        Promotion abgebrochen.")
            return 2
        print()
        print("  Curated-Refresh erfolgreich.")

    # Phase 2
    print_phase_header(2, "Quality Gate -- Great Expectations")
    try:
        success, stats = run_ge_checkpoint()
    except Exception as exc:
        print()
        print("FEHLER: GE-Checkpoint konnte nicht ausgefuehrt werden:")
        print(f"        {exc}")
        return 2

    print()
    print(f"  Expectations evaluiert: {stats['evaluated']}")
    print(f"  Erfolgreich:            {stats['successful']}")
    print(f"  Fehlgeschlagen:         {stats['unsuccessful']}")
    print()

    if not success:
        print("=" * 70)
        print("  GATE ROT -- Promotion blockiert.")
        print("=" * 70)
        print()
        print("  Trusted Layer wurde NICHT aktualisiert.")
        print("  Curated enthaelt Datenqualitaets-Issues, die in Trusted")
        print("  nicht erlaubt sind. Details siehe Data Docs:")
        print()
        print(f"  {GE_DIR / 'uncommitted' / 'data_docs' / 'local_site' / 'index.html'}")
        print()
        return 1

    print("  GATE GRUEN -- Promotion freigegeben.")

    # Phase 3
    print_phase_header(3, "Trusted-Promotion via dbt")
    rc = run_dbt_command(["run", "--select", "trusted_esg_emissions"])
    if rc != 0:
        print()
        print("FEHLER: dbt run fuer Trusted fehlgeschlagen.")
        print("        Gate war gruen, aber Build ging schief.")
        return 2

    print()
    print("=" * 70)
    print("  PROMOTION ERFOLGREICH.")
    print("=" * 70)
    print()
    print("  trusted_esg_emissions wurde aktualisiert.")
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
