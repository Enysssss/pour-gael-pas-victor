"""
Dataset schemas for the HealthAI Coach ETL pipeline.
Defines required columns, expected types and valid value sets per dataset.
"""

from typing import Any

# ---------------------------------------------------------------------------
# Required column definitions per dataset
# ---------------------------------------------------------------------------

REQUIRED_SCHEMAS: dict[str, list[str]] = {
    "users": [
        "email",
        "first_name",
        "last_name",
        "age",
        "gender",
        "weight_kg",
        "height_cm",
        "objective",
    ],
    "foods": [
        "name",
        "calories_kcal",
        "proteins_g",
        "carbs_g",
        "fats_g",
        "fiber_g",
        "sugars_g",
        "sodium_mg",
        "meal_type",
    ],
    "exercises": [
        "name",
        "category",
        "body_part",
        "equipment",
        "difficulty",
        "instructions",
    ],
    "metrics": [
        "user_id",
        "weight_kg",
        "bmi",
        "body_fat_pct",
        "heart_rate_avg",
        "heart_rate_max",
        "calories_burned",
        "workout_frequency",
        "water_intake_l",
    ],
}

# ---------------------------------------------------------------------------
# Numeric column bounds per dataset: col -> (min, max)
# ---------------------------------------------------------------------------

NUMERIC_BOUNDS: dict[str, dict[str, tuple[float, float]]] = {
    "users": {
        "age": (10, 100),
        "weight_kg": (1, 500),
        "height_cm": (50, 300),
    },
    "foods": {
        "calories_kcal": (0, 2000),
        "proteins_g": (0, 500),
        "carbs_g": (0, 500),
        "fats_g": (0, 500),
        "fiber_g": (0, 200),
        "sugars_g": (0, 500),
        "sodium_mg": (0, 50000),
    },
    "exercises": {},
    "metrics": {
        "bmi": (10, 60),
        "body_fat_pct": (0, 100),
        "heart_rate_avg": (40, 220),
        "heart_rate_max": (40, 220),
        "calories_burned": (0, 10000),
        "workout_frequency": (0, 14),
        "water_intake_l": (0, 20),
        "weight_kg": (1, 500),
    },
}

# ---------------------------------------------------------------------------
# Allowed categorical values per column (case-insensitive after normalization)
# ---------------------------------------------------------------------------

ALLOWED_VALUES: dict[str, dict[str, list[str]]] = {
    "users": {
        "gender": ["male", "female", "other"],
    },
    "foods": {
        "meal_type": ["breakfast", "lunch", "dinner", "snack", "other"],
    },
    "exercises": {
        "difficulty": ["beginner", "intermediate", "advanced"],
    },
    "metrics": {},
}

# ---------------------------------------------------------------------------
# PostgreSQL table mapping: dataset_type -> table_name
# ---------------------------------------------------------------------------

TABLE_MAPPING: dict[str, str] = {
    "users": "users",
    "foods": "foods",
    "exercises": "exercises",
    "metrics": "metrics",
}
