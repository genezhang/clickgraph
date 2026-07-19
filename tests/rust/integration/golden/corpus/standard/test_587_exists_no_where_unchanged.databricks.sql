SELECT 
      a.full_name AS `a.name`
FROM test_integration.users_test AS a
WHERE EXISTS (SELECT 1 FROM test_integration.user_follows_test WHERE user_follows_test.follower_id = a.user_id)
