SELECT 
      a.full_name AS "a.name"
FROM test_integration.users_test AS a
WHERE EXISTS (SELECT 1 FROM test_integration.user_follows_test AS e INNER JOIN test_integration.users_test AS b ON b.user_id = e.follower_id WHERE e.followed_id = a.user_id AND b.full_name = 'Bob Smith')
