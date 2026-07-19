SELECT 
      a.full_name AS `a.name`
FROM test_integration.users_test AS a
WHERE NOT EXISTS (SELECT 1 FROM test_integration.user_follows_test AS e INNER JOIN test_integration.users_test AS b ON b.user_id = e.followed_id WHERE e.follower_id = a.user_id AND b.full_name = 'Bob Smith')
