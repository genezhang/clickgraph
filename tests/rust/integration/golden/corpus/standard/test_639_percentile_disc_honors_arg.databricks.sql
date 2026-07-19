SELECT 
      percentile_disc(t0.followed_id, 0.9) AS `percentileDisc(b.user_id, 0.9)`
FROM test_integration.users_test AS a
INNER JOIN test_integration.user_follows_test AS t0 ON t0.follower_id = a.user_id
