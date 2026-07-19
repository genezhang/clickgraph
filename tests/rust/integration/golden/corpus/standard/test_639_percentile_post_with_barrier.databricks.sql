WITH with_city_p_cte_0 AS (SELECT 
      a.city AS `city`, 
      percentile_disc(t0.followed_id, 0.95) AS `p`
FROM test_integration.users_test AS a
INNER JOIN test_integration.user_follows_test AS t0 ON t0.follower_id = a.user_id
GROUP BY a.city
)
SELECT 
      city_p.city AS `city`, 
      city_p.p AS `p`
FROM with_city_p_cte_0 AS city_p
