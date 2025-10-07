#!/usr/bin/env python3
"""
validate_performance.py
========================
Performance and reliability assessment for the ETL workflow.
It measures runtime, memory usage, throughput, and error rates
for each ETL phase: Fedora → ResearchSpace and ResearchSpace → Omeka S.
"""

import time
import tracemalloc
import csv
import os
import logging
from collections import Counter
from orchestrator import run_phase

# Configuration 
LOG_DIR = os.path.join(os.getcwd(), "logs")
os.makedirs(LOG_DIR, exist_ok=True)
CSV_PATH = os.path.join(LOG_DIR, "performance_log.csv")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "performance_debug.log"), encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# Performance Wrapper
def measure_performance(phase_name: str, extra_args=""):
    """
    Measure execution time, memory, errors, and throughput for each phase.
    """
    logging.info(f"Starting performance test for phase: {phase_name}")
    start_time = time.perf_counter()
    tracemalloc.start()
    errors = Counter()
    processed = 0

    try:
        # Run the phase through orchestrator
        result = run_phase(phase_name, extra_args.split())

        # Try to infer processed count from results
        if isinstance(result, dict) and "processed" in result:
            processed = result.get("processed", 0)
        else:
            out_dir = "fedora_to_rspace/sparql_out" if "fedora" in phase_name else "rspace_to_omekas"
            if os.path.isdir(out_dir):
                processed = len([f for f in os.listdir(out_dir) if f.endswith(".ttl")])
    except Exception as e:
        errors[type(e).__name__] += 1
        logging.exception(f"Error during {phase_name}")
    finally:
        elapsed = time.perf_counter() - start_time
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

    avg_time = elapsed / processed if processed > 0 else 0
    throughput = processed / elapsed if elapsed > 0 else 0

    metrics = {
        "phase": phase_name,
        "elapsed_sec": round(elapsed, 2),
        "peak_mem_MB": round(peak / 1024 / 1024, 2),
        "processed": processed,
        "errors": sum(errors.values()),
        "error_types": dict(errors),
        "throughput": round(throughput, 2),
        "avg_time_per_res": round(avg_time, 3)
    }

    logging.info(f"Phase {phase_name} completed -a> {metrics}")
    return metrics

# CSV Writer
def write_to_csv(metrics):
    """
    Append results to CSV log file.
    """
    file_exists = os.path.isfile(CSV_PATH)
    with open(CSV_PATH, "a", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=list(metrics.keys()))
        if not file_exists:
            writer.writeheader()
        writer.writerow(metrics)

# Main
if __name__ == "__main__":
    logging.info("===== ETL Performance Validation Started =====")

    # Extra args per phase (same as your manual CLI usage)
    fedora_extra = (
        "--fedora-base https://datavault.ficlit.unibo.it/repo/rest "
        "--sparql-endpoint http://localhost:10215/blazegraph/namespace/kb/sparql "
        "--root-path UBOBU/MICROFILM/UBO8306198/402164/ "
        "--rules-file E:\\Workspace\\Ficlit-ETL\\workflows\\fedora_to_rspace\\rules.yaml "
        "--out-dir E:\\Workspace\\Ficlit-ETL\\workflows\\fedora_to_rspace\\sparql_out "
        "--named-graph http://datavault.ficlit.unibo.it/graph/microfilm "
        "--images-dir E:\\Workspace\\Ficlit-ETL\\researchspace-docker\\researchspace\\data\\images\\file "
        "--username fedoraAdmin --password fedoraAdmin "
        "--chunk-size 5000 --max-resources 5 -v"
    )

    rspace_extra = (
        "--config-path rspace_to_omekas/config_lab.json "
        "--max-resources 5 -v"
    )

    all_results = []
    for phase_name, extra_args in [
        ("fedora_to_rspace", fedora_extra),
        ("rspace_to_omekas", rspace_extra),
    ]:
        metrics = measure_performance(phase_name, extra_args=extra_args)
        write_to_csv(metrics)
        all_results.append(metrics)

    logging.info("===== Validation Complete =====")
    logging.info(f"Results stored at: {CSV_PATH}")
