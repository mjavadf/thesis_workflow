#!/usr/bin/env python3
"""
Step Manager – Fedora → ResearchSpace
=====================================

This orchestrator runs the two existing components in sequence:

1. etl_pipeline.py  (metadata → Turtle/TRiG/SPARQL INSERT + files.txt)
2. download_images.py (binary resources → JPGs into ResearchSpace tree)

It delegates the heavy lifting to those scripts (unchanged), but ensures
they run with consistent arguments and order.

Usage
-----
python step_manager_fedora_rs.py \
  --fedora-base https://datavault.ficlit.unibo.it/repo/rest \
  --root-path   UBOBU/MICROFILM/UBO8306198/402163/ \
  --rules-file  rules.yaml \
  --out-dir     sparql_out \
  --named-graph http://datavault.ficlit.unibo.it/graph/microfilm \
  --images-dir  E:\Workspace\Ficlit-ETL\researchspace-docker\researchspace\data\images\file \
  --username    [Auth_user] \
  --password    [Auth_pass] \
  --chunk-size  5000 \
  --max-resources 100 -v
  
  
powershell:
python step_manager_fedora_rs.py `
  --fedora-base https://datavault.ficlit.unibo.it/repo/rest `
  --root-path   UBOBU/MICROFILM/UBO8306198/402163/ `
  --rules-file  rules.yaml `
  --out-dir     sparql_out `
  --named-graph http://datavault.ficlit.unibo.it/graph/microfilm `
  --images-dir  "E:\Workspace\Ficlit-ETL\researchspace-docker\researchspace\data\images\file" `
  --username    [Auth_user] `
  --password    [Auth_pass] `
  --chunk-size  5000 `
  --max-resources 100
  --sparql-endpoint http://localhost:10215/blazegraph/namespace/kb/sparql `
  -v

"""

import argparse
import subprocess
import sys
from pathlib import Path
import requests


def run_cmd(cmd: list[str], desc: str):
    print(f"\n=== {desc} ===")
    print(" ".join(cmd))
    proc = subprocess.run(cmd)
    if proc.returncode != 0:
        sys.exit(f"✗ {desc} failed with code {proc.returncode}")
    print(f"✓ {desc} finished")


def push_to_sparql(endpoint: str, rq_dir: Path):
    """Post all insert-*.rq files to the given SPARQL endpoint."""
    rq_files = sorted(rq_dir.glob("insert-*.rq"))
    if not rq_files:
        print("⚠ No insert-*.rq files found for SPARQL insertion")
        return

    headers = {"Content-Type": "application/sparql-update"}
    for rq in rq_files:
        sparql = rq.read_text(encoding="utf-8")
        print(f"→ Inserting {rq.name} into {endpoint}")
        resp = requests.post(endpoint, data=sparql.encode("utf-8"), headers=headers)
        if resp.status_code in (200, 204):
            print(f"   ✓ Inserted {rq.name}")
        else:
            print(f"   ✗ Failed {rq.name}: {resp.status_code} {resp.text}")


def main():
    ap = argparse.ArgumentParser(description="Step manager for Fedora→ResearchSpace ETL")
    ap.add_argument("--fedora-base", required=True)
    ap.add_argument("--root-path", required=True)
    ap.add_argument("--named-graph", required=True)
    ap.add_argument("--rules-file", default="rules.yaml")
    ap.add_argument("--out-dir", default="sparql_out")
    ap.add_argument("--images-dir", required=True, help="Where to store converted JPGs")
    ap.add_argument("--username", required=True)
    ap.add_argument("--password", required=True)
    ap.add_argument("--chunk-size", type=int, default=10000)
    ap.add_argument("--max-resources", type=int, default=0)
    ap.add_argument("--sparql-endpoint", help="If set, insert generated SPARQL into this endpoint")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    files_txt = out_dir / "files.txt"

    # 1. Run metadata pipeline
    run_cmd(
        [
            sys.executable, "metadata_manager.py",
            "--fedora-base", args.fedora_base,
            "--root-path", args.root_path,
            "--rules-file", args.rules_file,
            "--out-dir", str(out_dir),
            "--named-graph", args.named_graph,
            "--username", args.username,
            "--password", args.password,
            "--chunk-size", str(args.chunk_size),
            "--max-resources", str(args.max_resources),
        ] + (["-v"] if args.verbose else []),
        "Metadata ETL"
    )

    # 2. If SPARQL endpoint is given, push generated insert-*.rq
    if args.sparql_endpoint:
        push_to_sparql(args.sparql_endpoint, out_dir)

    if not files_txt.exists():
        sys.exit("✗ No files.txt found (did metadata_manager.py produce it?)")

    # 3. Run digital assets pipeline
    run_cmd(
        [
            sys.executable, "digital_asset_manager.py",
            "--files", str(files_txt),
            "--out-dir", args.images_dir,
            "--username", args.username,
            "--password", args.password,
            "--workers", "5",
        ],
        "Binary Downloader"
    )

    print("\n✓ All steps completed successfully")


if __name__ == "__main__":
    main()
