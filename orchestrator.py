import argparse
import subprocess
import sys
from pathlib import Path

"""
usage:

python orchestrator.py --phase fedora_to_rspace `
  --fedora-base https://datavault.ficlit.unibo.it/repo/rest `
  --root-path   UBOBU/MICROFILM/UBO8306198/402155/ `
  --rules-file  rules.yaml `
  --out-dir     sparql_out `
  --named-graph http://datavault.ficlit.unibo.it/graph/microfilm `
  --images-dir  "/home/tech/RSpace/researchspace/data/images/file" `
  --username    [auth] `
  --password    [auth] `
  --chunk-size  5000 `
  --sparql-endpoint http://137.204.64.40:10215/blazegraph/namespace/kb/sparql `
  --max-resources 5 -v
  
python3 orchestrator.py --phase fedora_to_rspace \
  --fedora-base https://datavault.ficlit.unibo.it/repo/rest \
  --root-path UBOBU/MICROFILM/UBO8306198/402155/ \
  --rules-file rules.yaml \
  --out-dir sparql_out \
  --named-graph http://datavault.ficlit.unibo.it/graph/microfilm \
  --images-dir /home/tech/RSpace/researchspace/data/images/file \
  --username auth \
  --password auth \
  --chunk-size 5000 \
  --sparql-endpoint http://137.204.64.40:10215/blazegraph/namespace/kb/sparql \
  --max-resources 5 -v

python3 orchestrator.py --phase rspace_to_omekas \
  --rules-file rspace_to_omekas/rules_rs2os.yaml \
"""


def run_phase(phase: str, extra_args: list[str]):
    here = Path(__file__).resolve().parent

    if phase == "fedora_to_rspace":
        script = here / "fedora_to_rspace" / "step_manager_fedora_rs.py"
    elif phase == "rspace_to_omekas":
        script = here / "rspace_to_omekas" / "step_manager.py"
    else:
        sys.exit(f"Unknown phase: {phase}")

    # Ensure the working directory is the script’s folder
    cwd = script.parent

    cmd = [sys.executable, str(script)] + extra_args
    print(f"\n=== Running {phase} ===")
    print(" ".join(cmd))
    proc = subprocess.run(cmd, cwd=cwd)
    if proc.returncode != 0:
        sys.exit(proc.returncode)


def main():
    ap = argparse.ArgumentParser(
        description="ETL orchestrator: choose between Fedora→ResearchSpace or ResearchSpace→OmekaS"
    )
    ap.add_argument(
        "--phase",
        choices=["fedora_to_rspace", "rspace_to_omekas"],
        required=True,
        help="Which ETL phase to run",
    )

    args, extra = ap.parse_known_args()
    run_phase(args.phase, extra)


if __name__ == "__main__":
    main()