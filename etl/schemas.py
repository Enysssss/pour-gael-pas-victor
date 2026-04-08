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
        "meal_type": ["breakfast", "lunch", "dinner", "snack", "side", "other"],
    },
    "exercises": {
        "difficulty": ["beginner", "intermediate", "advanced"],
    },
    "metrics": {},
}

# ---------------------------------------------------------------------------
# Column aliases per dataset: raw_normalized_name -> canonical_name
#
# Applied during validation so that files with different naming conventions
# (e.g. "Protein (g)", "protein_g", "proteins_g") all map to the same
# canonical column expected by the clean + load stages.
# ---------------------------------------------------------------------------

COLUMN_ALIASES: dict[str, dict[str, str]] = {
    "users": {
        # common variants
        "firstname": "first_name",
        "lastname": "last_name",
        "user_email": "email",
        "height": "height_cm",
        "weight": "weight_kg",
        "goal": "objective",
    },
    "foods": {
        # nutrition.csv and similar exports
        "food_item": "name",
        "food_name": "name",
        "item": "name",
        "calories_kcal": "calories_kcal",           # already correct
        "protein_g": "proteins_g",
        "proteins_g": "proteins_g",                 # already correct
        "carbohydrates_g": "carbs_g",
        "carbs_g": "carbs_g",                       # already correct
        "fat_g": "fats_g",
        "fats_g": "fats_g",                         # already correct
        "fiber_g": "fiber_g",                       # already correct
        "sugars_g": "sugars_g",                     # already correct
        "sodium_mg": "sodium_mg",                   # already correct
        "cholesterol_mg": "cholesterol_mg",         # extra col, tolerated
        "water_intake_ml": "water_intake_ml",       # extra col, tolerated
        "category": "category",                     # extra col, tolerated
        "meal_type": "meal_type",                   # already correct
    },
    "exercises": {
        "exercise_name": "name",
        "muscle_group": "body_part",
        "level": "difficulty",
        "gear": "equipment",
        "desc": "instructions",
        "description": "instructions",
    },
    "metrics": {
        "userid": "user_id",
        "bodyfat": "body_fat_pct",
        "body_fat": "body_fat_pct",
        "hr_avg": "heart_rate_avg",
        "hr_max": "heart_rate_max",
        "hr_resting": "heart_rate_resting",
        "kcal_burned": "calories_burned",
        "workout_days": "workout_frequency",
        "water_l": "water_intake_l",
    },
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
