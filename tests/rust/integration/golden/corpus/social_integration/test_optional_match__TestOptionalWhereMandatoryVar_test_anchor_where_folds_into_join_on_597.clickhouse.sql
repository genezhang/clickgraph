SELECT 
      a.user_id AS "a.user_id", 
      t0.followed_id AS "b.user_id"
FROM test_integration.users_test AS a
LEFT JOIN test_integration.user_follows_test AS t0 ON t0.follower_id = a.user_id AND a.is_active = true
ORDER BY a.user_id ASC, t0.followed_id ASC
