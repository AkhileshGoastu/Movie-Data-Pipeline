# Movie Data Pipeline - tsworks Assignment

## Overview
This project builds a simple ETL pipeline that:
- Ingests MovieLens `movies.csv` and `ratings.csv`,
- Enriches movie metadata using the OMDb API,
- Stores cleaned & normalized data into a SQLite database,
- Provides SQL queries to answer analytics questions.

Files:
- `etl.py` — main ETL script
- `schema.sql` — database schema (CREATE TABLE statements)
- `queries.sql` — SQL queries answering the requested questions
- `requirements.txt` — Python dependencies
- `omdb_cache.json` — runtime cache generated after first run (not checked in)

## Design choices & assumptions
- Database: **SQLite** chosen for simplicity and portability. Easy to switch to PostgreSQL/MySQL by adapting connection & upsert logic.
- Movie uniqueness:
  - Prefer `imdb_id` when available (most reliable).
  - Fallback uniqueness on `movieId` + `title`.
- Genres normalized into a `genres` table; same for `directors`.
- Ratings loaded as-is. Deduplicated by `(userId, movieId, rating, timestamp)` to avoid duplicates across runs.
- OMDb enrichment is best-effort: if OMDb doesn't find a movie by title+year, the script retries by title alone. Missing OMDb fields set to `NULL`.
- API key is required (OMDb free tier). Set it via environment variable `OMDB_API_KEY`.

## Setup & Run
1. Clone repository (or copy files into a folder).
2. Install dependencies:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
"# Movie-Data-Pipeline" 
"# Movie-Data-Pipeline" 
