WITH with_w1 AS (
SELECT 
      1 AS "__row"
WHERE false
)
SELECT 
      count(*) AS "total"
FROM with_w1 AS w1
WHERE false

-- column total: Value [("total", "total")]