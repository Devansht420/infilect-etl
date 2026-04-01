# Infilect Data Ingestion Service

A FastAPI backend that accepts CSV uploads for retail master data, validates every row, resolves lookup foreign keys via get-or-create, and bulk inserts valid rows into PostgreSQL. Invalid rows are skipped and reported with exact row numbers, column names, and reasons.

---

## Tech Stack

| Layer | Choice |
|---|---|
| Framework | FastAPI (async) |
| Database | PostgreSQL 16 |
| ORM | SQLAlchemy 2.0 (async) |
| Validation | Pydantic v2 |
| CSV Parsing | pandas (chunked) |
| Package Manager | uv |
| Runtime | Python 3.12 |
| Containerisation | Docker + Docker Compose |

---

## Project Structure

```
app/
├── main.py              # FastAPI app, lifespan, router registration
├── config.py            # Settings from .env via pydantic-settings
├── database.py          # Async engine, session, create_all on startup
├── models/
│   ├── lookup.py        # 6 lookup tables (store_brands, cities, etc.)
│   ├── store.py
│   ├── user.py
│   └── pjp.py           # store-user mapping (permanent_journey_plans)
├── schemas/
│   ├── store_schema.py  # Pydantic row validator for stores CSV
│   ├── user_schema.py
│   └── pjp_schema.py
├── services/
│   ├── lookup_service.py   # get_or_create with in-memory cache + normalization
│   ├── store_ingestor.py   # validate → resolve FKs → bulk insert
│   ├── user_ingestor.py    # two-pass insert to handle supervisor self-reference
│   └── pjp_ingestor.py    # FK resolution against existing stores + users
└── routers/
    ├── stores.py
    ├── users.py
    └── pjp.py
```

---

## Local Setup

### Prerequisites

- Docker Desktop running
- `uv` installed: `brew install uv`

### Steps

```bash
# 1. Clone the repo
git clone <your-repo-url>
cd infilect-data-ingestion

# 2. Copy env file and edit if needed (defaults work out of the box)
cp .env.example .env

# 3. Generate the lockfile (required by Dockerfile)
uv lock

# 4. Build and start both services (DB + API)
docker compose up --build
```

The API is ready when you see:
```
infilect_api | INFO:     Application startup complete.
infilect_api | INFO:     Uvicorn running on http://0.0.0.0:8000
```

Tables are created automatically on startup. No migrations needed.

### Verify

```bash
# Health check
curl http://localhost:8000/health

# Interactive API docs
open http://localhost:8000/docs
```

---

## Docker Commands

```bash
# Start (no rebuild)
docker compose up

# Start with rebuild (after adding new packages)
docker compose up --build

# Stop (data preserved)
docker compose down

# Full reset — wipe DB and start fresh
docker compose down -v && docker compose up --build

# Live logs
docker compose logs -f api
docker compose logs -f db

# Connect to DB directly
docker exec -it infilect_db psql -U infilect_user -d infilect_db
```

---

## API Endpoints

Upload order matters — stores and users must be uploaded before PJP.

| Method | Endpoint | Purpose |
|---|---|---|
| GET | `/health` | Health check |
| POST | `/api/upload/stores` | Upload stores_master.csv |
| POST | `/api/upload/users` | Upload users_master.csv |
| POST | `/api/upload/pjp` | Upload store_user_mapping.csv |

### Postman Setup

1. Method: `POST`
2. URL: `http://localhost:8000/api/upload/stores`
3. Body → form-data → Key: `file`, Type: `File`, Value: select your CSV

### Download error report as JSON file

In Postman, click **Save Response → Save to a file** after sending.

### Response Format

```json
{
  "total_rows": 100,
  "inserted": 91,
  "failed": 9,
  "errors": [
    { "row": 7,  "column": "store_id",  "reason": "duplicate store_id 'STR-0004'" },
    { "row": 12, "column": "store_id",  "reason": "store_id is required" },
    { "row": 18, "column": "latitude",  "reason": "latitude must be a number, got 'not_available'" }
  ]
}
```

Row numbers are 1-based and include the header row, matching line numbers in any text editor.

---

## Validation Rules

**Stores** — required fields, store_id must match `STR-XXXX` pattern, latitude in [-90, 90], longitude in [-180, 180], name max 255 chars, no blank titles, duplicate store_id rejected.

**Users** — required fields, email format, phone format, user_type must be one of {1, 2, 3, 7}, username max 150 chars, duplicate usernames rejected. Supervisor resolved in a second pass after all users are inserted.

**PJP** — username and store_id must exist in DB, date must be valid YYYY-MM-DD, is_active must be True/False, duplicate (user, store, date) triplets rejected.

**All lookup fields** (city, state, country, region, store_brand, store_type) are normalized — stripped of whitespace and title-cased — before get-or-create, so `"  star bazaar  "` and `"STAR BAZAAR"` resolve to the same row.

---

## Performance — 500K Row File

Tested with `stores_master_500k.csv` (500,000 rows):

| Metric | Result |
|---|---|
| Total rows | 500,000 |
| Inserted | 491,449 |
| Failed | 8,551 |
| Total time | **36.45 seconds** |
| Response size | 774.27 KB |

**How it handles large files:**
- pandas `read_csv` with `chunksize=5000` — never loads the full file into memory
- In-memory lookup cache per request — same city/brand in 50,000 rows costs 1 DB query, not 50,000
- SQLAlchemy `insert()` bulk API — one INSERT statement per chunk, not per row
- Chunk-level commits — each chunk of 5,000 rows is its own transaction

---

## Failure Policy

Bad rows are skipped and valid rows are ingested. A 500K file with 8,551 bad rows should not block 491,449 valid ones. The complete error report tells the client exactly which rows failed and why, so they can correct and re-upload just those rows.

If a chunk fails mid-insert, only that chunk is rolled back — earlier committed chunks remain. This is intentional: chunks are small (5,000 rows), so the blast radius is limited and row numbers in the error report make it easy to identify where to resume.
