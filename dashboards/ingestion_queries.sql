-- Business-side queries on the loaded data. Connect BigQuery to Looker Studio
-- (Add data -> BigQuery -> Custom query) to turn these into charts.

-- 1. Orders and revenue per day
SELECT DATE(created_at) AS day, COUNT(*) AS orders, ROUND(SUM(amount), 2) AS revenue
FROM `raw.orders`
GROUP BY day
ORDER BY day DESC;

-- 2. Top customers
SELECT customer_id, COUNT(*) AS orders, ROUND(SUM(amount), 2) AS revenue
FROM `raw.orders`
GROUP BY customer_id
ORDER BY revenue DESC
LIMIT 10;

-- 3. Load jobs from the subscriber (freshness and failures, region-EU example)
SELECT creation_time, job_id, state, error_result.message AS error
FROM `region-eu`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE job_type = 'LOAD' AND job_id LIKE 'load_%'
ORDER BY creation_time DESC
LIMIT 50;
