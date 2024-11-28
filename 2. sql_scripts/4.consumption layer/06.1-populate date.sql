select max(event_date) from cricket.clean.match_detail_clean;
WITH RECURSIVE date_series AS (
    -- Anchor query: Start with the minimum event_date
    SELECT MIN(event_date) AS date
    FROM cricket.clean.match_detail_clean

    UNION ALL

    -- Recursive query: Increment the date by one day
    SELECT DATEADD(day, 1, date)
    FROM date_series
    WHERE date < (SELECT MAX(event_date) FROM cricket.clean.match_detail_clean)
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY date) AS date_id,
    date AS full_dt,
    EXTRACT(day FROM date) AS day,
    EXTRACT(month FROM date) AS month,
    EXTRACT(year FROM date) AS year,
    EXTRACT(quarter FROM date) AS quarter,
    DAYOFWEEKISO(date) AS dayofweek,
    EXTRACT(day FROM date) AS dayofmonth,
    DAYOFYEAR(date) AS dayofyear,
    DAYNAME(date) AS dateofweekname,
    CASE WHEN DAYNAME(date) IN ('Sat', 'Sun') THEN 1 ELSE 0 END AS isweekend
FROM date_series;


INSERT INTO cricket.consumption.date_dim
WITH RECURSIVE date_series AS (
    SELECT MIN(event_date) AS date
    FROM cricket.clean.match_detail_clean
    UNION ALL
    SELECT DATEADD(day, 1, date)
    FROM date_series
    WHERE date < (SELECT MAX(event_date) FROM cricket.clean.match_detail_clean)
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY date) AS date_id,
    date AS full_dt,
    EXTRACT(day FROM date) AS day,
    EXTRACT(month FROM date) AS month,
    EXTRACT(year FROM date) AS year,
    EXTRACT(quarter FROM date) AS quarter,
    DAYOFWEEKISO(date) AS dayofweek,
    EXTRACT(day FROM date) AS dayofmonth,
    DAYOFYEAR(date) AS dayofyear,
    DAYNAME(date) AS dateofweekname,
    CASE WHEN DAYNAME(date) IN ('Sat', 'Sun') THEN 1 ELSE 0 END AS isweekend
FROM date_series;

