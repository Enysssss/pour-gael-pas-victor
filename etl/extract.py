"""
Extract stage — reads a CSV, JSON or XLSX file into a pandas DataFrame.
Raises InvalidFileFormatError for unsupported extensions.
"""

import logging
import pathlib

import pandas as pd

from .exceptions import InvalidFileFormatError

logger = logging.getLogger("etl")

# Supported extensions mapped to their pandas reader
_READERS = {
    ".csv": pd.read_csv,
    ".json": pd.read_json,
    ".xlsx": pd.read_excel,
}


def extract(file_path: str) -> pd.DataFrame:
    """
    Read *file_path* into a DataFrame.

    Parameters
    ----------
    file_path:
        Absolute or relative path to the source file.

    Returns
    -------
    pd.DataFrame
        Raw, unmodified data from the source file.

    Raises
    ------
    InvalidFileFormatError
        If the file extension is not in (.csv, .json, .xlsx).
    FileNotFoundError
        If the file does not exist on disk.
    """
    path = pathlib.Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"Source file not found: {file_path}")

    ext = path.suffix.lower()
    reader = _READERS.get(ext)

    if reader is None:
        raise InvalidFileFormatError(
            file_path=str(file_path),
            message=f"Extension '{ext}' is not supported. Accepted: .csv .json .xlsx.",
        )

    logger.info("extract | file=%s format=%s", path.name, ext)

    try:
        df: pd.DataFrame = reader(file_path)
    except Exception as exc:
        logger.error("extract | failed to read %s — %s", file_path, exc)
        raise

    logger.info("extract | rows_raw=%d columns=%d", len(df), len(df.columns))
    return df
