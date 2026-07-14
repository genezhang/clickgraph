SELECT 
      a.full_name AS "a.name", 
      b.full_name AS "b.name"
FROM test_integration.users_test AS a
INNER JOIN test_integration.user_follows_test AS r1 ON a.user_id = r1.follower_id
INNER JOIN test_integration.user_follows_test AS r2 ON r1.followed_id = r2.follower_id
INNER JOIN test_integration.users_test AS b ON r2.followed_id = b.user_id
WHERE NOT (r1.follower_id = r2.follower_id AND r1.followed_id = r2.followed_id)
LIMIT 10