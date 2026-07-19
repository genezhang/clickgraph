SELECT 
      a.full_name AS "a.name", 
      stddevSamp(t0.followed_id) AS "stDev(b.user_id)"
FROM test_integration.users_test AS a
INNER JOIN test_integration.user_follows_test AS t0 ON t0.follower_id = a.user_id
GROUP BY a.full_name
