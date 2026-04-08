"""
Validate stage — checks that the DataFrame contains the expected columns
for the given dataset type.  Unknown extra columns are tolerated but logged.
Raises SchemaValidationError when required columns are missing.
"""

import logging

import pandas as pd

from .exceptions import SchemaValidationError
from .schemas import REQUIRED_SCHEMAS

logger = logging.getLogger("etl")


def validate(df: pd.DataFrame, dataset_type: str) -> pd.DataFrame:
    """
    Validate that *df* contains every required column for *dataset_type*.

    Parameters
    ----------
    df:
        Raw DataFrame produced by the extract stage.
    dataset_type:
        One of "users", "foods", "exercises", "metrics".

    Returns
    -------
    pd.DataFrame
        The same DataFrame (unchanged — validation is non-destructive).

    Raises
    ------
    SchemaValidationError
        If one or more required columns are absent.
    ValueError
        If *dataset_type* is unknown.
    """
    if dataset_type not in REQUIRED_SCHEMAS:
        raise ValueError(
            f"Unknown dataset_type '{dataset_type}'. "
            f"Valid values: {list(REQUIRED_SCHEMAS.keys())}"
        )

    required = set(REQUIRED_SCHEMAS[dataset_type])
    present = set(df.columns.str.lower().str.strip())

    # Normalise column names in-place for downstream stages
    df.columns = df.columns.str.lower().str.strip()

    missing = sorted(required - present)
    extra = sorted(present - required)

    if extra:
        logger.warning(
            "validate | dataset=%s unknown_columns=%s (tolerated)", dataset_type, extra
        )

    if missing:
        logger.error(
            "validate | dataset=%s missing_columns=%s", dataset_type, missing
        )
        raise SchemaValidationError(dataset_type=dataset_type, missing_columns=missing)

    logger.info(
        "validate | dataset=%s columns_ok=%d", dataset_type, len(required)
    )
    return df
