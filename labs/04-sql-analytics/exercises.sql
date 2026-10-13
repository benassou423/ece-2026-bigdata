-- Lab: SQL analytics with DuckDB, exercises
-- Run from the project root, after the "Load the tables" section of the lab:
--   duckdb analytics.duckdb -init init.sql -c ".read labs/04-sql-analytics/exercises.sql"

-- Exercise 1: average number of orders per user, and average quantity per order
SELECT
  round(count(*) / count(DISTINCT user_uuid), 2) AS avg_orders_per_user,
  round(avg(quantity), 2) AS avg_quantity_per_order
FROM orders;

-- Exercise 2: first and last order of each user, and the number of days between them
SELECT
  u.username,
  min(o.date)::DATE AS first_order,
  max(o.date)::DATE AS last_order,
  date_diff('day', min(o.date), max(o.date)) AS days_between
FROM orders o
JOIN users u ON o.user_uuid = u.uuid
GROUP BY u.username
ORDER BY first_order;

-- Exercise 3: hour of the day with the highest quantity sold
SELECT hour(date) AS hour, count(*) AS orders, sum(quantity) AS quantity
FROM orders
GROUP BY hour
ORDER BY quantity DESC
LIMIT 1;

-- Exercise 4: month-over-month variation of the quantity sold per product, in percent
WITH monthly AS (
  SELECT product, strftime(date, '%Y-%m') AS month, sum(quantity) AS quantity
  FROM orders
  GROUP BY ALL
)
SELECT
  product,
  month,
  quantity,
  lag(quantity) OVER w AS previous,
  round(100.0 * (quantity - lag(quantity) OVER w) / lag(quantity) OVER w, 1) AS mom_pct
FROM monthly
WINDOW w AS (PARTITION BY product ORDER BY month)
ORDER BY product, month;

-- Exercise 5: users who ordered every product at least once
SELECT u.username, u.name
FROM orders o
JOIN users u ON o.user_uuid = u.uuid
GROUP BY u.username, u.name
HAVING count(DISTINCT o.product) = (SELECT count(DISTINCT product) FROM orders)
ORDER BY u.username;
