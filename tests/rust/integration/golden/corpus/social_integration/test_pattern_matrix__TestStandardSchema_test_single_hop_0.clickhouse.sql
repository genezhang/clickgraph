SELECT 
      a.is_active AS "a.is_active", 
      b.is_active AS "b.is_active"
FROM test_integration.users_test AS a
INNER JOIN test_integration.user_follows_test AS r ON r.follower_id = a.user_id
INNER JOIN test_integration.users_test AS b ON b.user_id = r.followed_id
LIMIT 10