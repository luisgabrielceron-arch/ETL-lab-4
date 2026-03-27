from __future__ import annotations

import json
import subprocess
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Dict, List


PROJECT_ROOT = Path(__file__).resolve().parent.parent
REPORTS_DIR = PROJECT_ROOT / "reports"
SRC_DIR = PROJECT_ROOT / "src"


def now_iso() -> str:
    return datetime.now().isoformat()


def ensure_reports_dir() -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def log_line(message: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {message}")


def save_run_report(payload: Dict) -> Path:
    ensure_reports_dir()
    run_id = payload.get("run_id", datetime.now().strftime("%Y%m%d_%H%M%S_%f"))
    path = REPORTS_DIR / f"pipeline_run_{run_id}.json"
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return path


def run_step(step_key: str, step_name: str, script_name: str, status: List[Dict]) -> None:
    start = time.perf_counter()
    record: Dict = {
        "step_key": step_key,
        "step_name": step_name,
        "script": script_name,
        "started_at": now_iso(),
        "finished_at": None,
        "duration_seconds": None,
        "status": "running",
        "return_code": None,
        "error_type": None,
        "error_message": None,
        "traceback": None,
    }
    status.append(record)

    script_path = SRC_DIR / script_name
    log_line(f"START {step_key} - {step_name} ({script_name})")

    try:
        result = subprocess.run(
            [sys.executable, "-u", str(script_path)],
            cwd=str(PROJECT_ROOT),
            capture_output=False,
            text=True,
            check=False,
        )
        record["return_code"] = int(result.returncode)
        if result.returncode != 0:
            record["status"] = "failed"
            record["error_type"] = "SubprocessError"
            record["error_message"] = f"Step exited with code {result.returncode}"
            raise RuntimeError(record["error_message"])
        record["status"] = "success"
        log_line(f"OK    {step_key} - {step_name}")
    except Exception as e:
        record["status"] = "failed"
        record["error_type"] = type(e).__name__
        record["error_message"] = str(e)
        record["traceback"] = traceback.format_exc()
        log_line(f"FAIL  {step_key} - {step_name} | {record['error_type']}: {record['error_message']}")
        raise
    finally:
        end = time.perf_counter()
        record["finished_at"] = now_iso()
        record["duration_seconds"] = round(end - start, 6)


def run_pipeline() -> Path:
    ensure_reports_dir()
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S_%f")

    steps = [
        ("A", "Extract & Profiling", "extract.py"),
        ("B", "Input Validation (Great Expectations)", "validate_input.py"),
        ("C", "Data Quality Analysis & Policy Proposal", "quality_analysis.py"),
        ("D", "Cleaning", "clean.py"),
        ("E", "Transformation", "transform.py"),
        ("F", "Output Validation (Great Expectations)", "validate_output.py"),
        ("G", "Dimensional Model (Star Schema)", "dimensional_model.py"),
        ("H", "Load Data Warehouse (SQLite)", "load_dw.py"),
        ("I", "Business Analysis & KPI Charts", "analysis.py"),
    ]

    run_status: List[Dict] = []
    payload: Dict = {
        "run_id": run_id,
        "started_at": now_iso(),
        "finished_at": None,
        "status": "running",
        "steps": run_status,
    }

    report_path: Path
    try:
        for step_key, step_name, script_name in steps:
            run_step(step_key, step_name, script_name, run_status)
        payload["status"] = "success"
    except Exception:
        payload["status"] = "failed"
        raise
    finally:
        payload["finished_at"] = now_iso()
        report_path = save_run_report(payload)
        log_line(f"Pipeline run report: {report_path}")

    return report_path


if __name__ == "__main__":
    run_pipeline()
