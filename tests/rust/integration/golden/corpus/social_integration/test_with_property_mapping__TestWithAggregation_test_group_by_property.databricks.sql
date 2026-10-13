WITH with_w1 AS (
SELECT 
      v0.country AS `v1`, 
      count(*) AS `v2`
FROM test_integration.users_test AS v0
GROUP BY v0.country
)
SELECT 
      w1.v1 AS `country`, 
      w1.v2 AS `cnt`
FROM with_w1 AS w1
LIMIT 5