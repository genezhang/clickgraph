SELECT 
      a.is_active AS "a.is_active", 
      c.is_active AS "c.is_active"
FROM test_integration.users_test AS a
INNER JOIN test_integration.user_follows_test AS r1 ON r1.follower_id = a.user_id
INNER JOIN test_integration.user_follows_test AS r2 ON r2.follower_id = r1.followed_id
INNER JOIN test_integration.users_test AS c ON c.user_id = r2.followed_id
LIMIT 5