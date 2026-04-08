"""
Clean stage — applies generic + dataset-specific data quality rules.

Returns a tuple (cleaned_df, rejected_count) so the pipeline can report KPIs.
"""

import logging
import re
from typing import Tuple

import pandas as pd

from .schemas import ALLOWED_VALUES, NUMERIC_BOUNDS

logger = logging.getLogger("etl")

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def clean(df: pd.DataFrame, dataset_type: str) -> Tuple[pd.DataFrame, int]:
    """
    Apply all cleaning rules to *df* for the given *dataset_type*.

    Returns
    -------
    (cleaned_df, rejected_count)
        cleaned_df    — DataFrame ready for loading
        rejected_count — number of rows dropped during cleaning
    """
    original_len = len(df)
    logger.info("clean | dataset=%s rows_in=%d", dataset_type, original_len)

    df = _generic_clean(df)

    _cleaner = {
        "users": _clean_users,
        "foods": _clean_foods,
        "exercises": _clean_exercises,
        "metrics": _clean_metrics,
    }.get(dataset_type)

    if _cleaner:
        df = _cleaner(df)
    else:
        logger.warning("clean | no specific cleaner for dataset '%s'", dataset_type)

    rejected = original_len - len(df)
    logger.info(
        "clean | dataset=%s rows_out=%d rejected=%d", dataset_type, len(df), rejected
    )
    return df, rejected


# ---------------------------------------------------------------------------
# Generic cleaning (applied to every dataset)
# ---------------------------------------------------------------------------


def _generic_clean(df: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicates, normalise column names and trim string whitespace."""
    before = len(df)

    # Normalise column names to snake_case
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(r"\s+", "_", regex=True)
        .str.replace(r"[^\w]", "_", regex=True)
    )

    # Trim string columns
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(lambda s: s.str.strip() if s.dtype == "object" else s)

    # Drop full duplicates
    df = df.drop_duplicates()
    dupes_dropped = before - len(df)
    if dupes_dropped:
        logger.info("clean | duplicates_dropped=%d", dupes_dropped)

    return df.reset_index(drop=True)


def _apply_numeric_bounds(
    df: pd.DataFrame, dataset_type: str
) -> pd.DataFrame:
    """Drop rows whose numeric columns fall outside the allowed bounds."""
    bounds = NUMERIC_BOUNDS.get(dataset_type, {})
    for col, (lo, hi) in bounds.items():
        if col not in df.columns:
            continue
        df[col] = pd.to_numeric(df[col], errors="coerce")
        before = len(df)
        df = df[df[col].between(lo, hi, inclusive="both") | df[col].isna()]
        dropped = before - len(df)
        if dropped:
            logger.warning(
                "clean | col=%s bound=[%s,%s] rows_dropped=%d", col, lo, hi, dropped
            )
    return df


def _standardise_categoricals(
    df: pd.DataFrame, dataset_type: str
) -> pd.DataFrame:
    """Normalise categorical columns to lowercase and drop out-of-vocabulary rows."""
    allowed_map = ALLOWED_VALUES.get(dataset_type, {})
    for col, allowed in allowed_map.items():
        if col not in df.columns:
            continue
        df[col] = df[col].astype(str).str.lower().str.strip()
        before = len(df)
        df = df[df[col].isin(allowed) | df[col].isna()]
        dropped = before - len(df)
        if dropped:
            logger.warning(
                "clean | col=%s invalid_values_dropped=%d allowed=%s",
                col,
                dropped,
                allowed,
            )
    return df


# ---------------------------------------------------------------------------
# Dataset-specific cleaners
# ---------------------------------------------------------------------------


def _clean_users(df: pd.DataFrame) -> pd.DataFrame:
    """USERS specific rules."""
    # Email validation — keep rows with a syntactically valid email
    if "email" in df.columns:
        email_pattern = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
        before = len(df)
        df = df[df["email"].apply(lambda v: bool(email_pattern.match(str(v))))]
        dropped = before - len(df)
        if dropped:
            logger.warning("clean | users invalid_emails_dropped=%d", dropped)

    # Drop nulls on critical columns
    critical = ["email", "first_name", "last_name"]
    df = df.dropna(subset=[c for c in critical if c in df.columns])

    # Cast numeric columns
    for col in ("age", "weight_kg", "height_cm"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Numeric bounds (age 10-100, weight > 0, height > 0)
    df = _apply_numeric_bounds(df, "users")

    # Gender normalisation
    df = _standardise_categoricals(df, "users")

    # Objective fallback
    if "objective" in df.columns:
        df["objective"] = df["objective"].fillna("general_fitness").str.lower().str.strip()

    return df.reset_index(drop=True)


def _clean_foods(df: pd.DataFrame) -> pd.DataFrame:
    """FOODS specific rules."""
    numeric_cols = ["calories_kcal", "proteins_g", "carbs_g", "fats_g",
                    "fiber_g", "sugars_g", "sodium_mg"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df = _apply_numeric_bounds(df, "foods")
    df = _standardise_categoricals(df, "foods")

    # Drop rows without a name
    if "name" in df.columns:
        df = df.dropna(subset=["name"])
        df = df[df["name"].str.strip() != ""]

    return df.reset_index(drop=True)


def _clean_exercises(df: pd.DataFrame) -> pd.DataFrame:
    """EXERCISES specific rules."""
    # Normalise difficulty
    df = _standardise_categoricals(df, "exercises")

    # Normalise equipment — lowercase and strip
    if "equipment" in df.columns:
        df["equipment"] = (
            df["equipment"].astype(str).str.lower().str.strip().replace("nan", "none")
        )

    # Drop rows without a name or instructions
    for col in ("name", "instructions"):
        if col in df.columns:
            df = df.dropna(subset=[col])
            df = df[df[col].str.strip() != ""]

    return df.reset_index(drop=True)


def _clean_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """METRICS specific rules."""
    numeric_cols = [
        "weight_kg", "bmi", "body_fat_pct",
        "heart_rate_avg", "heart_rate_max", "calories_burned",
        "workout_frequency", "water_intake_l",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # user_id is mandatory
    if "user_id" in df.columns:
        df = df.dropna(subset=["user_id"])

    df = _apply_numeric_bounds(df, "metrics")

    return df.reset_index(drop=True)
