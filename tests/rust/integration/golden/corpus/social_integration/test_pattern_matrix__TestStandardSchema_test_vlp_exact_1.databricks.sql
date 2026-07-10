SELECT 
      a.user_id AS `a.user_id`, 
      r2.followed_id AS `b.user_id`
FROM test_integration.users_test AS a
INNER JOIN test_integration.user_follows_test AS r1 ON a.user_id = r1.follower_id
INNER JOIN test_integration.user_follows_test AS r2 ON r1.followed_id = r2.follower_id
WHERE a.user_id <> r2.followed_id
LIMIT 10