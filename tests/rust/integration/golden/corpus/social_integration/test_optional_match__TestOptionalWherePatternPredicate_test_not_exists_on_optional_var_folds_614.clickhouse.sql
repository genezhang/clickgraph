SELECT 
      a.user_id AS "a.user_id", 
      b.user_id AS "b.user_id"
FROM test_integration.users_test AS a
LEFT JOIN (SELECT t0.follower_id AS __cg_combined_anchor_key, b.* FROM test_integration.user_follows_test AS t0 JOIN test_integration.users_test AS b ON b.user_id = t0.followed_id WHERE NOT EXISTS (SELECT 1 FROM test_integration.user_follows_test WHERE user_follows_test.follower_id = b.user_id)) AS b ON b.__cg_combined_anchor_key = a.user_id
ORDER BY a.user_id ASC, b.user_id ASC
