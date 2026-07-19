SELECT 
      a.user_id AS "a.user_id"
FROM test_integration.users_test AS a
WHERE NOT EXISTS (SELECT 1 FROM test_integration.user_follows_test WHERE user_follows_test.follower_id = a.user_id AND user_follows_test.followed_id = b.user_id)
