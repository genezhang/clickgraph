WITH with_a_cte_0 AS (SELECT *
FROM social.users_bench AS u
)
SELECT 
      sum(a.a) AS `s`
FROM with_a_cte_0 AS a
