WITH with_w1 AS (
SELECT 
      CASE WHEN count(*) >= 0 THEN 0 ELSE 0 END AS "v4"
WHERE false
HAVING count(*) > 0
)
SELECT 
      NULL AS "prop", 
      w1.v4 AS "cnt"
FROM with_w1 AS w1
ORDER BY w1.v4 DESC NULLS FIRST
LIMIT 10
-- column prop: Value [("prop", "prop")]
-- column cnt: Value [("cnt", "cnt")]