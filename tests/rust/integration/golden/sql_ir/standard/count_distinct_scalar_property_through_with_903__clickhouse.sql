WITH with_a_cte_0 AS (SELECT 
      u.age AS "a"
FROM social.users_bench AS u
)
SELECT 
      count(DISTINCT a.a) AS "c"
FROM with_a_cte_0 AS a
