-- queries.sql

-- 1) Which movie has the highest average rating?
SELECT m.title, AVG(r.rating) AS avg_rating
FROM ratings r
JOIN movies m ON r.movie_id = m.movie_id
GROUP BY m.title
ORDER BY avg_rating DESC
LIMIT 1;

-- 2) Top 5 movie genres with highest average rating

SELECT m.genre, AVG(r.rating) AS avg_rating
FROM ratings r
JOIN movies m ON r.movie_id = m.movie_id
GROUP BY m.genre
ORDER BY avg_rating DESC
LIMIT 5;

-- 3) Director with the most movies in dataset
SELECT director, COUNT(*) AS movie_count
FROM movies
GROUP BY director
ORDER BY movie_count DESC
LIMIT 1;

-- 4) Average rating of movies released each year
SELECT m.year, AVG(r.rating) AS avg_rating
FROM ratings r
JOIN movies m ON r.movie_id = m.movie_id
GROUP BY m.year
ORDER BY m.year;
