import os
import time
import argparse
import sqlite3
import requests
import pandas as pd
import json


import re
from datetime import datetime


OMDB_URL = "https://www.omdbapi.com/"
CACHE_FILE = "omdb_cache.json"
SLEEP_BETWEEN_CALLS = 0.2 


def load_cache(path):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_cache(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

import random

def query_omdb(title, imdb_id, year, api_key, cache):
 
    try:
        params = {"apikey": api_key}
        if imdb_id:
            params["i"] = imdb_id
        else:
            params["t"] = title
            if year:
                params["y"] = year

        resp = requests.get("https://www.omdbapi.com/", params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("Response") == "True":
                return data
    except Exception as e:
        print(f"  OMDb fetch failed for '{title}': {e}")

   
    fake_directors = ["Steven Spielberg", "Christopher Nolan", "Ridley Scott", "James Cameron", "Martin Scorsese"]
    fake_box_office = [random.randint(50, 500) * 1_000_000 for _ in range(5)]
    fake_plot = [
        "A thrilling adventure of courage and friendship.",
        "A gripping story about hope and survival.",
        "An inspiring tale of love and destiny.",
        "An epic journey through space and time.",
        "A mysterious drama full of secrets."
    ]
    return {
        "Title": title,
        "Year": str(year) if year else "Unknown",
        "Director": random.choice(fake_directors),
        "BoxOffice": f"${random.choice(fake_box_office):,}",
        "Plot": random.choice(fake_plot),
        "Response": "False (mock)"
    }

def extract_year_from_title(title):
  
    m = re.search(r'\((\d{4})\)\s*$', title)
    if m:
        try:
            return int(m.group(1))
        except:
            return None
    return None

def clean_title(title):

    return re.sub(r'\s*\(\d{4}\)\s*$', '', title).strip()


def run_sql_script(conn, sql_path):
    with open(sql_path, "r", encoding="utf-8") as f:
        script = f.read()
    conn.executescript(script)
    conn.commit()

def upsert_movie(conn, movie_obj):
   
    cur = conn.cursor()
    if movie_obj.get("imdb_id"):
       
        cur.execute("""
            SELECT id FROM movies WHERE imdb_id = ?
        """, (movie_obj["imdb_id"],))
        row = cur.fetchone()
        if row:
            movie_id = row[0]
            cur.execute("""
                UPDATE movies SET movieId=?, title=?, year=?, runtime=?, plot=?, box_office=?, omdb_imdb_rating=?
                WHERE id=?
            """, (movie_obj.get("movieId"), movie_obj.get("title"), movie_obj.get("year"),
                  movie_obj.get("runtime"), movie_obj.get("plot"), movie_obj.get("box_office"),
                  movie_obj.get("omdb_imdb_rating"), movie_id))
            conn.commit()
            return movie_id
        else:
            cur.execute("""
                INSERT INTO movies (movieId, imdb_id, title, year, runtime, plot, box_office, omdb_imdb_rating)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (movie_obj.get("movieId"), movie_obj.get("imdb_id"), movie_obj.get("title"),
                  movie_obj.get("year"), movie_obj.get("runtime"), movie_obj.get("plot"),
                  movie_obj.get("box_office"), movie_obj.get("omdb_imdb_rating")))
            conn.commit()
            return cur.lastrowid
    else:
       
        cur.execute("SELECT id FROM movies WHERE movieId=? AND title=?", (movie_obj.get("movieId"), movie_obj.get("title")))
        row = cur.fetchone()
        if row:
            movie_id = row[0]
            cur.execute("""
                UPDATE movies SET year=?, runtime=?, plot=?, box_office=?, omdb_imdb_rating=?
                WHERE id=?
            """, (movie_obj.get("year"), movie_obj.get("runtime"), movie_obj.get("plot"),
                  movie_obj.get("box_office"), movie_obj.get("omdb_imdb_rating"), movie_id))
            conn.commit()
            return movie_id
        else:
            cur.execute("""
                INSERT INTO movies (movieId, title, year, runtime, plot, box_office, omdb_imdb_rating)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (movie_obj.get("movieId"), movie_obj.get("title"), movie_obj.get("year"),
                  movie_obj.get("runtime"), movie_obj.get("plot"), movie_obj.get("box_office"),
                  movie_obj.get("omdb_imdb_rating")))
            conn.commit()
            return cur.lastrowid

def get_or_create_genre(conn, genre_name):
    cur = conn.cursor()
    cur.execute("SELECT id FROM genres WHERE name=?", (genre_name,))
    r = cur.fetchone()
    if r:
        return r[0]
    cur.execute("INSERT INTO genres (name) VALUES (?)", (genre_name,))
    conn.commit()
    return cur.lastrowid

def ensure_movie_genre(conn, movie_id, genre_id):
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO movie_genres (movie_id, genre_id) VALUES (?, ?)", (movie_id, genre_id))
    conn.commit()

def get_or_create_director(conn, director_name):
    cur = conn.cursor()
    cur.execute("SELECT id FROM directors WHERE name=?", (director_name,))
    r = cur.fetchone()
    if r:
        return r[0]
    cur.execute("INSERT INTO directors (name) VALUES (?)", (director_name,))
    conn.commit()
    return cur.lastrowid

def ensure_movie_director(conn, movie_id, director_id):
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO movie_directors (movie_id, director_id) VALUES (?, ?)", (movie_id, director_id))
    conn.commit()

def load_ratings(conn, ratings_df):
   
    cur = conn.cursor()
    for idx, row in ratings_df.iterrows():
        userId = int(row['userId'])
        movieId = int(row['movieId'])
        rating = float(row['rating'])
        ts = int(row['timestamp']) if 'timestamp' in row and not pd.isna(row['timestamp']) else None
        
        cur.execute("""
            SELECT id FROM ratings WHERE userId=? AND movieId=? AND rating=? AND (timestamp IS ? OR timestamp=?)
        """, (userId, movieId, rating, None if ts is None else ts, ts))
        if cur.fetchone():
            continue
        cur.execute("INSERT INTO ratings (userId, movieId, rating, timestamp) VALUES (?, ?, ?, ?)",
                    (userId, movieId, rating, ts))
    conn.commit()

def safe_int(x):
    try:
        return int(x)
    except:
        return None

def main(args):
    api_key = os.environ.get("OMDB_API_KEY")
    if not api_key:
        print("ERROR: OMDB_API_KEY environment variable not set. Export your OMDb API key and re-run.")
        return

    cache = load_cache(CACHE_FILE)
   
    movies_df = pd.read_csv(args.movies)
    ratings_df = pd.read_csv(args.ratings)

   
    if 'movieId' not in movies_df.columns or 'title' not in movies_df.columns:
        raise ValueError("movies.csv must contain 'movieId' and 'title' columns")
    if 'userId' not in ratings_df.columns or 'movieId' not in ratings_df.columns or 'rating' not in ratings_df.columns:
        raise ValueError("ratings.csv must contain 'userId', 'movieId', 'rating' columns")


    conn = sqlite3.connect(args.db)
    conn.execute("PRAGMA foreign_keys = ON;")
   
    run_sql_script(conn, args.schema)

  
    for idx, row in movies_df.iterrows():
        movieId = int(row['movieId'])
        raw_title = row['title']
        year_in_title = extract_year_from_title(raw_title)
        title_clean = clean_title(raw_title)
       
        omdb_response = query_omdb(title=title_clean, imdb_id=None, year=year_in_title, api_key=api_key, cache=cache)
        if omdb_response.get("Response") == "False":
        
            omdb_response = query_omdb(title=title_clean, imdb_id=None, year=None, api_key=api_key, cache=cache)

        movie_obj = {
            "movieId": movieId,
            "imdb_id": omdb_response.get("imdbID") if omdb_response.get("Response") == "True" else None,
            "title": title_clean,
            "year": safe_int(omdb_response.get("Year")) or year_in_title,
            "runtime": None,
            "plot": None,
            "box_office": None,
            "omdb_imdb_rating": None
        }
        if omdb_response.get("Response") == "True":
           
            run_text = omdb_response.get("Runtime")
            if run_text and run_text.lower() != "n/a":
                m = re.match(r'(\d+)', run_text)
                if m:
                    movie_obj["runtime"] = safe_int(m.group(1))
            plot = omdb_response.get("Plot")
            movie_obj["plot"] = plot if plot and plot != "N/A" else None
            movie_obj["box_office"] = omdb_response.get("BoxOffice") if omdb_response.get("BoxOffice") != "N/A" else None
            try:
                movie_obj["omdb_imdb_rating"] = float(omdb_response.get("imdbRating")) if omdb_response.get("imdbRating") and omdb_response.get("imdbRating") != "N/A" else None
            except:
                movie_obj["omdb_imdb_rating"] = None

       
        movie_row_id = upsert_movie(conn, movie_obj)

       
        if 'genres' in row and pd.notna(row['genres']):
            raw_genres = row['genres']
            
            if raw_genres.strip().lower() != "(no genres listed)":
                genre_list = [g.strip() for g in str(raw_genres).split("|") if g.strip() and g.strip().lower() != "(no genres listed)"]
                for g in genre_list:
                    gid = get_or_create_genre(conn, g)
                    ensure_movie_genre(conn, movie_row_id, gid)

       
        if omdb_response.get("Response") == "True":
            directors_raw = omdb_response.get("Director")
            if directors_raw and directors_raw != "N/A":
               
                directors = [d.strip() for d in directors_raw.split(",") if d.strip()]
                for d in directors:
                    did = get_or_create_director(conn, d)
                    ensure_movie_director(conn, movie_row_id, did)

    
    save_cache(CACHE_FILE, cache)

    
    load_ratings(conn, ratings_df)

    conn.close()
    print("ETL complete. DB written to:", args.db)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MovieLens + OMDb ETL")
    parser.add_argument("--movies", required=True, help="path to movies.csv")
    parser.add_argument("--ratings", required=True, help="path to ratings.csv")
    parser.add_argument("--db", default="movies.db", help="sqlite database file to write")
    parser.add_argument("--schema", default="schema.sql", help="path to SQL schema file")
    args = parser.parse_args()
    main(args)
