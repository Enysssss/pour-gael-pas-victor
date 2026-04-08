"""
HealthAI Coach — FastAPI backend
=================================
Provides:
  - ETL upload endpoints (POST /upload/*)
  - CRUD read endpoints (GET /users, /foods, /exercises, /metrics, /sessions)
  - Export endpoints (GET /export/*/csv|json)
  - Monitoring endpoints (GET /etl/health, /etl/logs, /quality/report)

Security: x-api-key header (set API_KEY in .env).
"""

import logging
import logging.config
import os
import pathlib
import shutil
import time
from contextlib import asynccontextmanager
from typing import Any, Optional

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, File, Header, HTTPException, Query, UploadFile
from fastapi.responses import JSONResponse, StreamingResponse

from etl.pipeline import run_pipeline

# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------

load_dotenv()

BASE_DIR = pathlib.Path(__file__).parent
UPLOADS_DIR = BASE_DIR / "uploads"
EXPORTS_DIR = BASE_DIR / "exports"
LOGS_DIR = BASE_DIR / "logs"
LOG_FILE = LOGS_DIR / "etl.log"

UPLOADS_DIR.mkdir(exist_ok=True)
EXPORTS_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)

# ---------------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------------

logging.config.dictConfig(
    {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "standard": {
                "format": "%(asctime)s [%(levelname)s] %(name)s | %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            }
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "standard",
            },
            "file": {
                "class": "logging.FileHandler",
                "filename": str(LOG_FILE),
                "formatter": "standard",
                "encoding": "utf-8",
            },
        },
        "loggers": {
            "etl": {"handlers": ["console", "file"], "level": "INFO", "propagate": False},
            "app": {"handlers": ["console", "file"], "level": "INFO", "propagate": False},
        },
    }
)

app_logger = logging.getLogger("app")

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

_DEFAULT_DSN = "postgresql+psycopg2://postgres:postgres@mainline.proxy.rlwy.net:51566/healthai"


def _db_dsn() -> str:
    raw = os.getenv("DATABASE_URL", _DEFAULT_DSN)
    return raw.replace("postgresql+psycopg2://", "postgresql://")


def _db_conn():
    """Return a fresh psycopg2 connection (caller is responsible for closing)."""
    return psycopg2.connect(_db_dsn())


def _query(sql: str, params: tuple = ()) -> list[dict]:
    """Execute a SELECT query and return rows as list-of-dicts."""
    conn = _db_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Security dependency
# ---------------------------------------------------------------------------

API_KEY = os.getenv("API_KEY", "changeme")


def require_api_key(x_api_key: str = Header(..., alias="x-api-key")) -> None:
    """FastAPI dependency — validates the x-api-key header."""
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing API key.")


# ---------------------------------------------------------------------------
# App lifecycle
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    app_logger.info("HealthAI Coach API starting up")
    yield
    app_logger.info("HealthAI Coach API shutting down")


# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------

app = FastAPI(
    title="HealthAI Coach — ETL & REST API",
    description=(
        "Industrialised ETL pipeline + REST API for the HealthAI Coach MSPR project. "
        "Handles ingestion of CSV/JSON/XLSX datasets (users, foods, exercises, metrics), "
        "data quality cleaning, PostgreSQL loading, and read/export endpoints."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

# ---------------------------------------------------------------------------
# ── UPLOAD / ETL endpoints ──────────────────────────────────────────────────
# ---------------------------------------------------------------------------

ALLOWED_EXTENSIONS = {".csv", ".json", ".xlsx"}


def _save_upload(file: UploadFile) -> pathlib.Path:
    """Persist the uploaded file to the uploads/ directory and return its path."""
    dest = UPLOADS_DIR / file.filename
    with dest.open("wb") as f:
        shutil.copyfileobj(file.file, f)
    return dest


def _upload_handler(file: UploadFile, dataset_type: str) -> dict[str, Any]:
    """Shared logic for all upload routes."""
    suffix = pathlib.Path(file.filename).suffix.lower()
    if suffix not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=422,
            detail=f"Unsupported file format '{suffix}'. Accepted: {ALLOWED_EXTENSIONS}",
        )

    dest = _save_upload(file)
    app_logger.info("upload | dataset=%s file=%s", dataset_type, dest.name)

    report = run_pipeline(str(dest), dataset_type)

    if report["status"] == "error":
        raise HTTPException(status_code=422, detail=report)

    return report


@app.post(
    "/upload/users",
    summary="Upload users dataset",
    tags=["ETL Upload"],
    dependencies=[Depends(require_api_key)],
)
def upload_users(file: UploadFile = File(...)):
    """
    Upload a CSV / JSON / XLSX file containing user profiles.
    Runs the full ETL pipeline and returns an execution report.
    """
    return _upload_handler(file, "users")


@app.post(
    "/upload/foods",
    summary="Upload foods dataset",
    tags=["ETL Upload"],
    dependencies=[Depends(require_api_key)],
)
def upload_foods(file: UploadFile = File(...)):
    """Upload a CSV / JSON / XLSX file containing food items."""
    return _upload_handler(file, "foods")


@app.post(
    "/upload/exercises",
    summary="Upload exercises dataset",
    tags=["ETL Upload"],
    dependencies=[Depends(require_api_key)],
)
def upload_exercises(file: UploadFile = File(...)):
    """Upload a CSV / JSON / XLSX file containing exercise definitions."""
    return _upload_handler(file, "exercises")


@app.post(
    "/upload/metrics",
    summary="Upload biometric metrics dataset",
    tags=["ETL Upload"],
    dependencies=[Depends(require_api_key)],
)
def upload_metrics(file: UploadFile = File(...)):
    """Upload a CSV / JSON / XLSX file containing user biometric metrics."""
    return _upload_handler(file, "metrics")


# ---------------------------------------------------------------------------
# ── CRUD READ endpoints ─────────────────────────────────────────────────────
# ---------------------------------------------------------------------------


def _paginate(sql_base: str, limit: int, offset: int) -> list[dict]:
    """Append LIMIT / OFFSET to a base SELECT and execute."""
    sql = f"{sql_base} LIMIT %s OFFSET %s"
    return _query(sql, (limit, offset))


@app.get("/users", summary="List users", tags=["CRUD"])
def get_users(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    _: None = Depends(require_api_key),
):
    """Return a paginated list of user profiles."""
    return _paginate('SELECT * FROM "users"', limit, offset)


@app.get("/foods", summary="List foods", tags=["CRUD"])
def get_foods(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    _: None = Depends(require_api_key),
):
    """Return a paginated list of food items."""
    return _paginate('SELECT * FROM "foods"', limit, offset)


@app.get("/exercises", summary="List exercises", tags=["CRUD"])
def get_exercises(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    _: None = Depends(require_api_key),
):
    """Return a paginated list of exercise definitions."""
    return _paginate('SELECT * FROM "exercises"', limit, offset)


@app.get("/metrics", summary="List biometric metrics", tags=["CRUD"])
def get_metrics(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    _: None = Depends(require_api_key),
):
    """Return a paginated list of biometric metrics."""
    return _paginate('SELECT * FROM "metrics"', limit, offset)


@app.get("/sessions", summary="List training sessions", tags=["CRUD"])
def get_sessions(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    _: None = Depends(require_api_key),
):
    """Return a paginated list of training sessions."""
    return _paginate('SELECT * FROM "sessions"', limit, offset)


# ---------------------------------------------------------------------------
# ── EXPORT endpoints ────────────────────────────────────────────────────────
# ---------------------------------------------------------------------------


def _export_csv(table: str) -> StreamingResponse:
    """Stream a full table as CSV."""
    import io

    import pandas as pd

    conn = _db_conn()
    try:
        df = pd.read_sql(f'SELECT * FROM "{table}"', conn)
    finally:
        conn.close()

    buf = io.StringIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    return StreamingResponse(
        buf,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={table}.csv"},
    )


def _export_json_response(table: str):
    """Return a full table as JSON."""
    rows = _query(f'SELECT * FROM "{table}"')
    return JSONResponse(content=rows)


@app.get("/export/users/csv", summary="Export users as CSV", tags=["Export"])
def export_users_csv(_: None = Depends(require_api_key)):
    """Download the full users table as a CSV file."""
    return _export_csv("users")


@app.get("/export/users/json", summary="Export users as JSON", tags=["Export"])
def export_users_json(_: None = Depends(require_api_key)):
    """Return the full users table as JSON."""
    return _export_json_response("users")


@app.get("/export/foods/csv", summary="Export foods as CSV", tags=["Export"])
def export_foods_csv(_: None = Depends(require_api_key)):
    """Download the full foods table as a CSV file."""
    return _export_csv("foods")


@app.get("/export/foods/json", summary="Export foods as JSON", tags=["Export"])
def export_foods_json(_: None = Depends(require_api_key)):
    """Return the full foods table as JSON."""
    return _export_json_response("foods")


@app.get("/export/exercises/csv", summary="Export exercises as CSV", tags=["Export"])
def export_exercises_csv(_: None = Depends(require_api_key)):
    """Download the full exercises table as a CSV file."""
    return _export_csv("exercises")


@app.get("/export/metrics/csv", summary="Export metrics as CSV", tags=["Export"])
def export_metrics_csv(_: None = Depends(require_api_key)):
    """Download the full metrics table as a CSV file."""
    return _export_csv("metrics")


# ---------------------------------------------------------------------------
# ── MONITORING endpoints ────────────────────────────────────────────────────
# ---------------------------------------------------------------------------


@app.get("/etl/health", summary="ETL pipeline health check", tags=["Monitoring"])
def etl_health():
    """
    Verifies database connectivity and reports API status.
    No API key required — usable by infrastructure health probes.
    """
    try:
        conn = _db_conn()
        conn.close()
        db_ok = True
        db_error = None
    except Exception as exc:
        db_ok = False
        db_error = str(exc)

    return {
        "status": "ok" if db_ok else "degraded",
        "database": "connected" if db_ok else f"error: {db_error}",
        "log_file": str(LOG_FILE),
        "uploads_dir": str(UPLOADS_DIR),
    }


@app.get("/etl/logs", summary="Return recent ETL log lines", tags=["Monitoring"])
def etl_logs(
    lines: int = Query(100, ge=1, le=1000),
    _: None = Depends(require_api_key),
):
    """Return the last *lines* lines of the ETL log file."""
    if not LOG_FILE.exists():
        return {"lines": []}

    with LOG_FILE.open("r", encoding="utf-8") as f:
        all_lines = f.readlines()

    return {"lines": [line.rstrip() for line in all_lines[-lines:]]}


@app.get("/quality/report", summary="Data quality KPI report", tags=["Monitoring"])
def quality_report(_: None = Depends(require_api_key)):
    """
    Parses the ETL log to compute quality KPIs per dataset:
    - total executions
    - total rows processed / inserted / rejected
    - rejection rate (%)
    - insert rate (%)
    - error count
    """
    import re

    if not LOG_FILE.exists():
        return {"datasets": {}}

    pattern = re.compile(
        r"pipeline \| END dataset=(\S+) status=(\S+) rows_raw=(\d+) rows_clean=(\d+) "
        r"rows_inserted=(\d+) rejected=(\d+)"
    )

    stats: dict[str, dict[str, Any]] = {}

    with LOG_FILE.open("r", encoding="utf-8") as f:
        for line in f:
            m = pattern.search(line)
            if not m:
                continue
            ds = m.group(1)
            status = m.group(2)
            raw = int(m.group(3))
            inserted = int(m.group(5))
            rejected = int(m.group(6))

            entry = stats.setdefault(
                ds,
                {
                    "executions": 0,
                    "rows_raw": 0,
                    "rows_inserted": 0,
                    "rejected_rows": 0,
                    "errors": 0,
                },
            )
            entry["executions"] += 1
            entry["rows_raw"] += raw
            entry["rows_inserted"] += inserted
            entry["rejected_rows"] += rejected
            if status != "success":
                entry["errors"] += 1

    for ds, entry in stats.items():
        raw_total = entry["rows_raw"]
        entry["rejection_rate_pct"] = (
            round(entry["rejected_rows"] / raw_total * 100, 2) if raw_total else 0.0
        )
        entry["insert_rate_pct"] = (
            round(entry["rows_inserted"] / raw_total * 100, 2) if raw_total else 0.0
        )

    return {"datasets": stats}


# ---------------------------------------------------------------------------
# Entry point (development)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
