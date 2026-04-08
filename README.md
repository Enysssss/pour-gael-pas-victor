# HealthAI Coach — ETL Pipeline + REST API

Backend industrialisé pour le projet MSPR / CDA **HealthAI Coach**.  
Ingestion automatisée CSV / JSON / XLSX → nettoyage qualité → PostgreSQL → API REST documentée.

---

## Architecture

```
project/
├── app.py                  # FastAPI — tous les endpoints
├── requirements.txt
├── .env.example            # Variables d'environnement
├── uploads/                # Fichiers uploadés (temporaire)
├── exports/                # Fichiers exportés
├── logs/
│   └── etl.log             # Journal d'exécution ETL
└── etl/
    ├── __init__.py
    ├── exceptions.py       # Exceptions métier custom
    ├── schemas.py          # Colonnes requises + bornes métier
    ├── extract.py          # Lecture CSV / JSON / XLSX
    ├── validate.py         # Contrôle de structure
    ├── clean.py            # Nettoyage + règles métier
    ├── load.py             # Insertion PostgreSQL (psycopg2)
    └── pipeline.py         # Orchestrateur extract→validate→clean→load
```

---

## Installation

### 1. Cloner le repo

```bash
git clone <repo-url>
cd project
```

### 2. Environnement virtuel

```bash
python -m venv .venv
source .venv/bin/activate        # Linux / Mac
.venv\Scripts\activate           # Windows
```

### 3. Dépendances

```bash
pip install -r requirements.txt
```

### 4. Configuration

```bash
cp .env.example .env
# Éditer .env : DATABASE_URL et API_KEY
```

---

## Lancement

```bash
uvicorn app:app --host 0.0.0.0 --port 8000 --reload
```

Swagger UI disponible sur : `http://localhost:8000/docs`  
ReDoc disponible sur : `http://localhost:8000/redoc`

---

## Docker (optionnel)

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
EXPOSE 8000
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
```

```bash
docker build -t healthai-backend .
docker run -p 8000:8000 --env-file .env healthai-backend
```

---

## Endpoints

### ETL Upload — `POST /upload/{dataset}`

| Endpoint            | Dataset   | Formats acceptés     |
|---------------------|-----------|----------------------|
| `POST /upload/users`     | Profils utilisateurs | CSV, JSON, XLSX |
| `POST /upload/foods`     | Aliments            | CSV, JSON, XLSX |
| `POST /upload/exercises` | Exercices           | CSV, JSON, XLSX |
| `POST /upload/metrics`   | Métriques biométriques | CSV, JSON, XLSX |

**Header requis :** `x-api-key: <votre_clé>`

**Réponse :**
```json
{
  "dataset": "users",
  "rows_raw": 120,
  "rows_clean": 118,
  "rows_inserted": 118,
  "rejected_rows": 2,
  "errors": [],
  "duration_seconds": 1.23,
  "status": "success"
}
```

---

### CRUD Lecture — `GET /{table}`

| Endpoint         | Description                    |
|------------------|-------------------------------|
| `GET /users`     | Liste paginée des utilisateurs |
| `GET /foods`     | Liste paginée des aliments     |
| `GET /exercises` | Liste paginée des exercices    |
| `GET /metrics`   | Liste paginée des métriques    |
| `GET /sessions`  | Liste paginée des sessions     |

**Paramètres de pagination :**
- `limit` (défaut: 50, max: 500)
- `offset` (défaut: 0)

---

### Export

| Endpoint                   | Format  |
|----------------------------|---------|
| `GET /export/users/csv`    | CSV     |
| `GET /export/users/json`   | JSON    |
| `GET /export/foods/csv`    | CSV     |
| `GET /export/foods/json`   | JSON    |
| `GET /export/exercises/csv`| CSV     |
| `GET /export/metrics/csv`  | CSV     |

---

### Monitoring

| Endpoint             | Description                              | Auth |
|----------------------|------------------------------------------|------|
| `GET /etl/health`    | Santé DB + API                           | Non  |
| `GET /etl/logs`      | Dernières lignes du log ETL              | Oui  |
| `GET /quality/report`| KPIs qualité agrégés par dataset         | Oui  |

---

## Exemples curl

### Upload CSV users

```bash
curl -X POST http://localhost:8000/upload/users \
  -H "x-api-key: changeme" \
  -F "file=@/chemin/vers/users.csv"
```

### Upload XLSX foods

```bash
curl -X POST http://localhost:8000/upload/foods \
  -H "x-api-key: changeme" \
  -F "file=@/chemin/vers/foods.xlsx"
```

### Lire les utilisateurs (page 2)

```bash
curl -X GET "http://localhost:8000/users?limit=20&offset=20" \
  -H "x-api-key: changeme"
```

### Exporter les aliments en CSV

```bash
curl -X GET http://localhost:8000/export/foods/csv \
  -H "x-api-key: changeme" \
  -o foods_export.csv
```

### Rapport qualité

```bash
curl -X GET http://localhost:8000/quality/report \
  -H "x-api-key: changeme"
```

### Health check (sans auth)

```bash
curl http://localhost:8000/etl/health
```

---

## Workflow ETL

```
Fichier uploadé (CSV/JSON/XLSX)
        │
        ▼
   extract()          ← détection format, lecture pandas
        │
        ▼
   validate()         ← colonnes requises présentes ?
        │
        ▼
    clean()           ← doublons, outliers, catégoriels, emails
        │
        ▼
     load()           ← INSERT batch psycopg2 + transaction
        │
        ▼
   Rapport JSON        ← rows_raw / rows_clean / inserted / rejected
```

---

## Schéma des datasets

### users
`email, first_name, last_name, age, gender, weight_kg, height_cm, objective`

### foods
`name, calories_kcal, proteins_g, carbs_g, fats_g, fiber_g, sugars_g, sodium_mg, meal_type`

### exercises
`name, category, body_part, equipment, difficulty, instructions`

### metrics
`user_id, weight_kg, bmi, body_fat_pct, heart_rate_avg, heart_rate_max, calories_burned, workout_frequency, water_intake_l`

---

## Règles qualité appliquées

| Dataset   | Règles                                                                |
|-----------|-----------------------------------------------------------------------|
| users     | email valide, age 10-100, poids/taille > 0, gender normalisé          |
| foods     | macros >= 0, calories <= 2000, meal_type standardisé                   |
| exercises | difficulty ∈ {beginner, intermediate, advanced}, equipment normalisé   |
| metrics   | bpm 40-220, bmi 10-60, body fat 0-100                                  |

---

## Extensibilité

Le pipeline est conçu pour une intégration future avec :
- **Apache Airflow** : remplacer `run_pipeline()` par un DAG task
- **Cron** : `0 2 * * * python -c "from etl.pipeline import run_pipeline; run_pipeline(...)"`
- **Module IA** : les données nettoyées dans PostgreSQL alimentent directement les modèles
- **Dashboard** : `/quality/report` et `/export/*` fournissent les données analytics
