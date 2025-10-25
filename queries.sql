-- queries.sql

-- 1) Which movie has the highest average rating?
-- We compute average rating per movieId and join to movies title.
SELECT m.title, m.year, AVG(r.rating) AS avg_rating, COUNT(r.id) AS num_ratings
FROM ratings r
JOIN movies m ON r.movieId = m.movieId
GROUP BY r.movieId
HAVING COUNT(r.id) >= 1
ORDER BY avg_rating DESC, num_ratings DESC
LIMIT 1;

-- 2) Top 5 movie genres that have the highest average rating
-- Average rating per genre (only include movies with at least 5 ratings to avoid tiny-sample noise)
SELECT g.name AS genre, AVG(r.rating) AS avg_rating, COUNT(r.id) AS num_ratings
FROM genres g
JOIN movie_genres mg ON g.id = mg.genre_id
JOIN movies m ON mg.movie_id = m.id
JOIN ratings r ON r.movieId = m.movieId
GROUP BY g.id
HAVING COUNT(r.id) >= 5
ORDER BY avg_rating DESC
LIMIT 5;

-- 3) Director with most movies in this dataset
SELECT d.name AS director, COUNT(md.movie_id) AS movie_count
FROM directors d
JOIN movie_directors md ON d.id = md.director_id
GROUP BY d.id
ORDER BY movie_count DESC
LIMIT 1;

-- 4) Average rating of movies released each year
SELECT m.year AS release_year, ROUND(AVG(r.rating), 3) AS avg_rating, COUNT(DISTINCT m.movieId) AS movies_count
FROM movies m
JOIN ratings r ON r.movieId = m.movieId
WHERE m.year IS NOT NULL
GROUP BY m.year
ORDER BY m.year ASC;
