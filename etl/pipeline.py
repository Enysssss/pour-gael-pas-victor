"""
Pipeline orchestrator — wires extract → validate → clean → load.

Returns a structured execution report used by the API and stored in the log.
"""

import logging
import time
from typing import Any

from .clean import clean
from .exceptions import DatabaseLoadError, InvalidFileFormatError, SchemaValidationError
from .extract import extract
from .load import load
from .schemas import TABLE_MAPPING
from .validate import validate

logger = logging.getLogger("etl")


def run_pipeline(file_path: str, dataset_type: str) -> dict[str, Any]:
    """
    Execute the full ETL pipeline for a single source file.

    Parameters
    ----------
    file_path:
        Path to the uploaded file (csv / json / xlsx).
    dataset_type:
        One of "users", "foods", "exercises", "metrics".

    Returns
    -------
    dict
        Execution report with rows_raw, rows_clean, rows_inserted,
        rejected_rows, errors, duration_seconds, status.
    """
    report: dict[str, Any] = {
        "dataset": dataset_type,
        "file": file_path,
        "rows_raw": 0,
        "rows_clean": 0,
        "rows_inserted": 0,
        "rejected_rows": 0,
        "errors": [],
        "duration_seconds": 0.0,
        "status": "error",
    }

    start = time.perf_counter()
    logger.info(
        "pipeline | START dataset=%s file=%s", dataset_type, file_path
    )

    try:
        # ── 1. Extract ────────────────────────────────────────────────────────
        df_raw = extract(file_path)
        report["rows_raw"] = len(df_raw)

        # ── 2. Validate ───────────────────────────────────────────────────────
        df_validated = validate(df_raw, dataset_type)

        # ── 3. Clean ──────────────────────────────────────────────────────────
        df_clean, rejected = clean(df_validated, dataset_type)
        report["rows_clean"] = len(df_clean)
        report["rejected_rows"] = rejected

        # ── 4. Load ───────────────────────────────────────────────────────────
        table_name = TABLE_MAPPING.get(dataset_type, dataset_type)
        inserted = load(df_clean, table_name)
        report["rows_inserted"] = inserted
        report["status"] = "success"

    except (InvalidFileFormatError, SchemaValidationError, DatabaseLoadError) as exc:
        report["errors"].append(str(exc))
        logger.error("pipeline | FAILED dataset=%s error=%s", dataset_type, exc)

    except Exception as exc:
        report["errors"].append(f"Unexpected error: {exc}")
        logger.exception("pipeline | UNEXPECTED ERROR dataset=%s", dataset_type)

    finally:
        report["duration_seconds"] = round(time.perf_counter() - start, 3)
        logger.info(
            "pipeline | END dataset=%s status=%s rows_raw=%d rows_clean=%d "
            "rows_inserted=%d rejected=%d duration=%.3fs",
            dataset_type,
            report["status"],
            report["rows_raw"],
            report["rows_clean"],
            report["rows_inserted"],
            report["rejected_rows"],
            report["duration_seconds"],
        )

    return report
