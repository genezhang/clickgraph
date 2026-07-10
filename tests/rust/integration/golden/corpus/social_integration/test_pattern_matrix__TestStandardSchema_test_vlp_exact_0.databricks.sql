SELECT 
      a.is_active AS `a.is_active`, 
      b.is_active AS `b.is_active`
FROM test_integration.users_test AS a
INNER JOIN test_integration.user_follows_test AS r1 ON a.user_id = r1.follower_id
INNER JOIN test_integration.user_follows_test AS r2 ON r1.followed_id = r2.follower_id
INNER JOIN test_integration.users_test AS b ON r2.followed_id = b.user_id
WHERE a.user_id <> b.user_id
LIMIT 10