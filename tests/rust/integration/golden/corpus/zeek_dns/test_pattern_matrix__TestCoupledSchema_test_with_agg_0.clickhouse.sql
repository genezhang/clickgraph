WITH with_w1 AS (
SELECT 
      v1.query AS "v3", 
      count(v1.uid) AS "v4"
FROM zeek.dns_log AS v1
WHERE (v1.query IS NOT NULL AND v1.answers IS NOT NULL)
GROUP BY v1.query
)
SELECT 
      w1.v3 AS "prop", 
      w1.v4 AS "cnt"
FROM with_w1 AS w1
ORDER BY w1.v4 DESC NULLS FIRST
LIMIT 10