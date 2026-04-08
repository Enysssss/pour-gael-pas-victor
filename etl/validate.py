"""
Validate stage — normalises column names, applies dataset-specific aliases,
then checks that all required columns are present.

Unknown extra columns are tolerated but logged.
Raises SchemaValidationError when required columns are still missing after aliasing.
"""

import logging
import re

import pandas as pd

from .exceptions import SchemaValidationError
from .schemas import COLUMN_ALIASES, REQUIRED_SCHEMAS

logger = logging.getLogger("etl")


def _normalize_col(name: str) -> str:
    """
    Convert a raw column name to a clean snake_case identifier.

    Examples
    --------
    'Calories (kcal)' → 'calories_kcal'
    'Protein (g)'     → 'protein_g'
    'Food_Item'       → 'food_item'
    '  First Name  '  → 'first_name'
    """
    name = name.strip().lower()
    # Replace any non-alphanumeric character (spaces, parens, dashes…) with _
    name = re.sub(r"[^\w]+", "_", name)
    # Collapse consecutive underscores
    name = re.sub(r"_+", "_", name)
    # Strip leading/trailing underscores
    name = name.strip("_")
    return name


def validate(df: pd.DataFrame, dataset_type: str) -> pd.DataFrame:
    """
    Normalise column names, apply dataset aliases, then validate required schema.

    Parameters
    ----------
    df:
        Raw DataFrame produced by the extract stage.
    dataset_type:
        One of "users", "foods", "exercises", "metrics".

    Returns
    -------
    pd.DataFrame
        DataFrame with normalised + aliased column names, ready for clean stage.

    Raises
    ------
    SchemaValidationError
        If one or more required columns are absent after aliasing.
    ValueError
        If *dataset_type* is unknown.
    """
    if dataset_type not in REQUIRED_SCHEMAS:
        raise ValueError(
            f"Unknown dataset_type '{dataset_type}'. "
            f"Valid values: {list(REQUIRED_SCHEMAS.keys())}"
        )

    # ── Step 1: normalize all column names to clean snake_case ──────────────
    df.columns = [_normalize_col(c) for c in df.columns]

    # ── Step 2: apply dataset-specific column aliases ────────────────────────
    aliases = COLUMN_ALIASES.get(dataset_type, {})
    df = df.rename(columns=aliases)

    # ── Step 3: check required columns ───────────────────────────────────────
    required = set(REQUIRED_SCHEMAS[dataset_type])
    present = set(df.columns)

    missing = sorted(required - present)
    extra = sorted(present - required)

    if extra:
        logger.info(
            "validate | dataset=%s extra_columns=%s (tolerated)", dataset_type, extra
        )

    if missing:
        logger.error(
            "validate | dataset=%s missing_columns=%s", dataset_type, missing
        )
        raise SchemaValidationError(dataset_type=dataset_type, missing_columns=missing)

    logger.info(
        "validate | dataset=%s OK — required=%d extra=%d",
        dataset_type,
        len(required),
        len(extra),
    )
    return df
