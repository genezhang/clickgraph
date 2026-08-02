WITH with_a_cte_0 AS (SELECT 
      u.age AS "a"
FROM test_integration.users_test AS u
), 
with_b_cte_1 AS (SELECT 
      a.a AS "b"
FROM with_a_cte_0 AS a
)
SELECT 
      sum(b.b) AS "total"
FROM with_b_cte_1 AS b
