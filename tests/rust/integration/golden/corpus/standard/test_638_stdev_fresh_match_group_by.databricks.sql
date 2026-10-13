SELECT 
      v0.full_name AS `a.name`, 
      stddev_samp(v2.user_id) AS `stDev(b.user_id)`
FROM test_integration.users_test AS v0
JOIN test_integration.user_follows_test AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.followed_id = v2.user_id
GROUP BY v0.full_name
