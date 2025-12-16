import os
import time
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from urllib.parse import quote_plus

password = quote_plus("Akhilesh@007") 
DB_URI = f"mysql+mysqlconnector://root:{password}@localhost:3306/movie_db"
OMDB_API_KEY = os.getenv("OMDB_API_KEY", "85e3c510")  # fallback API key
MOVIE_LIMIT = 500

def fetch_omdb(title, year=None):
    params = {"t": title, "apikey": OMDB_API_KEY}
    if year:
        params["y"] = str(year)
    try:
        r = requests.get("http://www.omdbapi.com/", params=params, timeout=5)
        data = r.json()
        return data if data.get("Response") == "True" else None
    except Exception:
        return None

def safe_int(v):
    try:
        return int(v)
    except Exception:
        return None

def run_etl(movies_csv="movies.csv", ratings_csv="ratings.csv"):
    print("Starting ETL...")

    movies = pd.read_csv(movies_csv)
    ratings = pd.read_csv(ratings_csv)

    if MOVIE_LIMIT:
        movies = movies.head(MOVIE_LIMIT)

    def parse_title_year(t):
        year = None
        title = t
        if t.endswith(")") and "(" in t:
            p = t.rsplit("(", 1)
            if len(p) == 2:
                title = p[0].strip()
                year = safe_int(p[1].replace(")", "").strip())
        return title, year

    movies[["clean_title", "year_parsed"]] = movies["title"].apply(
        lambda x: pd.Series(parse_title_year(x))
    )

    enriched = []
    for _, row in movies.iterrows():
        data = fetch_omdb(row["clean_title"], row["year_parsed"])
        enriched.append({
            "movieId": row["movie_id"],
            "director": data.get("Director") if data and data.get("Director") else None,
            "plot": data.get("Plot") if data and data.get("Plot") else None,
            "box_office": data.get("BoxOffice") if data and data.get("BoxOffice") else None
        })
        time.sleep(0.1) 

    enriched_df = pd.DataFrame(enriched)

    movies = movies.merge(
        enriched_df,
        left_on="movie_id",
        right_on="movieId",
        how="left"
    ).drop(columns=["movieId"], errors="ignore")

    movies.rename(columns={"genres": "genre", "year_parsed": "year"}, inplace=True)

    for col in ["director", "plot", "box_office"]:
        if f"{col}_y" in movies.columns:
            movies[col] = movies[f"{col}_y"].fillna(movies.get(col, "" if col == "plot" else "Unknown"))
        elif f"{col}_x" in movies.columns:
            movies[col] = movies[f"{col}_x"].fillna("" if col == "plot" else "Unknown")
        else:
            movies[col] = "" if col == "plot" else "Unknown"

    if "year" not in movies.columns:
        movies["year"] = 0
    movies["year"] = movies["year"].fillna(0).astype(int)

    ratings["timestamp"] = pd.to_datetime(ratings["timestamp"], errors="coerce")

    movies = movies.loc[:, ~movies.columns.duplicated()]

    try:
        engine = create_engine(DB_URI)
        with engine.connect() as conn:
            conn.execute(text("CREATE DATABASE IF NOT EXISTS movie_db"))

        movies[["movie_id", "title", "genre", "director", "plot", "box_office", "year"]].to_sql(
            "movies", engine, if_exists="replace", index=False
        )

        expected_cols = {"user_id", "movie_id", "rating", "timestamp"}
        if not expected_cols.issubset(ratings.columns):
            print("Ratings CSV columns:", ratings.columns)
            raise ValueError("Ratings CSV does not contain required columns.")

        ratings.to_sql("ratings", engine, if_exists="replace", index=False)

        print("Loaded tables: movies, ratings")

    except Exception as e:
        print("DB Error:", e)

if __name__ == "__main__":
    run_etl()
