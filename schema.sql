-- schema.sql
PRAGMA foreign_keys = ON;

-- Movies table (unique by imdb_id if available, else movieId + title)
CREATE TABLE IF NOT EXISTS movies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    movieId INTEGER, -- movieLens id
    imdb_id TEXT UNIQUE, -- e.g. tt0111161
    title TEXT NOT NULL,
    year INTEGER,
    runtime INTEGER,
    plot TEXT,
    box_office TEXT,
    omdb_imdb_rating REAL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(movieId, title) -- fallback uniqueness
);

-- Genres table (normalized)
CREATE TABLE IF NOT EXISTS genres (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL
);

-- association table movies <-> genres
CREATE TABLE IF NOT EXISTS movie_genres (
    movie_id INTEGER NOT NULL,
    genre_id INTEGER NOT NULL,
    PRIMARY KEY (movie_id, genre_id),
    FOREIGN KEY (movie_id) REFERENCES movies(id) ON DELETE CASCADE,
    FOREIGN KEY (genre_id) REFERENCES genres(id) ON DELETE CASCADE
);

-- Directors table
CREATE TABLE IF NOT EXISTS directors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL
);

-- association table movies <-> directors (a movie may have multiple directors)
CREATE TABLE IF NOT EXISTS movie_directors (
    movie_id INTEGER NOT NULL,
    director_id INTEGER NOT NULL,
    PRIMARY KEY (movie_id, director_id),
    FOREIGN KEY (movie_id) REFERENCES movies(id) ON DELETE CASCADE,
    FOREIGN KEY (director_id) REFERENCES directors(id) ON DELETE CASCADE
);

-- Ratings table (one row per user rating from ratings.csv)
CREATE TABLE IF NOT EXISTS ratings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    userId INTEGER NOT NULL,
    movieId INTEGER NOT NULL, -- movieLens movieId (foreign key not enforced to keep flexibility)
    rating REAL NOT NULL,
    timestamp INTEGER,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Index to speed up rating lookups
CREATE INDEX IF NOT EXISTS idx_ratings_movieId ON ratings(movieId);
