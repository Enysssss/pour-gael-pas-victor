"""
Custom exceptions for the HealthAI Coach ETL pipeline.
Each exception maps to a distinct failure stage in the pipeline.
"""


class InvalidFileFormatError(Exception):
    """Raised when the uploaded file format is not supported (not csv/json/xlsx)."""

    def __init__(self, file_path: str, message: str = "Unsupported file format."):
        self.file_path = file_path
        self.message = f"{message} File: {file_path}"
        super().__init__(self.message)


class SchemaValidationError(Exception):
    """Raised when required columns are missing or the dataset structure is invalid."""

    def __init__(self, dataset_type: str, missing_columns: list[str]):
        self.dataset_type = dataset_type
        self.missing_columns = missing_columns
        self.message = (
            f"Schema validation failed for dataset '{dataset_type}'. "
            f"Missing required columns: {missing_columns}"
        )
        super().__init__(self.message)


class DatabaseLoadError(Exception):
    """Raised when the database insertion fails (connection error, constraint violation, etc.)."""

    def __init__(self, table_name: str, detail: str):
        self.table_name = table_name
        self.detail = detail
        self.message = f"Failed to load data into table '{table_name}': {detail}"
        super().__init__(self.message)
