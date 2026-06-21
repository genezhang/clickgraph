WITH with_c_n_cte_0 AS (SELECT 
      u.country AS "c", 
      count(u.user_id) AS "n"
FROM social.users_bench AS u
GROUP BY u.country
HAVING n > 5
)
SELECT 
      c_n.c AS "c", 
      c_n.n AS "n"
FROM with_c_n_cte_0 AS c_n
