WITH with_a_cte_0 AS (SELECT 
      u.age AS `a`
FROM social.users_bench AS u
)
SELECT 
      avg(a.a) AS `s`
FROM with_a_cte_0 AS a
