WITH with_a_avg_age_cte_0 AS (SELECT 
      anyLast(a.name) AS "p1_a_name", 
      avg(b.age) AS "avg_age"
FROM test_integration.users AS a
INNER JOIN test_integration.follows AS t0 ON t0.follower_id = a.user_id
INNER JOIN test_integration.users AS b ON b.user_id = t0.followed_id
GROUP BY a.user_id
HAVING avg_age > 25
)
SELECT 
      a_avg_age.p1_a_name AS "a.name", 
      a_avg_age.avg_age AS "avg_age"
FROM with_a_avg_age_cte_0 AS a_avg_age
ORDER BY a_avg_age.p1_a_name ASC
