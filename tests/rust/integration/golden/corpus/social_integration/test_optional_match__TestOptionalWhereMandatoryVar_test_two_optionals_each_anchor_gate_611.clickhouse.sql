SELECT 
      a.user_id AS "a.user_id", 
      t0.followed_id AS "b.user_id", 
      t1.post_id AS "p.post_id"
FROM test_integration.users_test AS a
LEFT JOIN test_integration.user_follows_test AS t0 ON t0.follower_id = a.user_id AND a.is_active = true
LEFT JOIN test_integration.post_likes_test AS t1 ON t1.user_id = a.user_id AND a.country = 'US'
ORDER BY a.user_id ASC, t0.followed_id ASC, t1.post_id ASC
