-- schema.sql
CREATE TABLE movies (
    movie_id INT PRIMARY KEY,
    title VARCHAR(255),
    genre VARCHAR(255),
    director VARCHAR(255),
    plot TEXT,
    box_office VARCHAR(100),
    year INT
);

CREATE TABLE  ratings (
    user_id INT,
    movie_id INT,
    rating FLOAT,
    timestamp DATETIME,
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id)
);
