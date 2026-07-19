SELECT 
      c.full_name AS `c.name`, 
      n AS `n`
FROM test_integration.users_test AS a
INNER JOIN test_integration.user_follows_test AS t0 ON t0.follower_id = a.user_id
INNER JOIN test_integration.user_follows_test AS t1 ON t1.follower_id = t0.followed_id
INNER JOIN test_integration.users_test AS c ON c.user_id = t1.followed_id
LATERAL VIEW explode(array(1, 2)) AS n
WHERE NOT t1.follow_id = t0.follow_id
UNION ALL 
SELECT 
      c.full_name AS `c.name`, 
      n AS `n`
FROM test_integration.users_test AS b
INNER JOIN test_integration.user_follows_test AS t0 ON t0.follower_id = b.user_id
INNER JOIN test_integration.user_follows_test AS t1 ON t1.follower_id = b.user_id
INNER JOIN test_integration.users_test AS c ON c.user_id = t1.followed_id
LATERAL VIEW explode(array(1, 2)) AS n
WHERE NOT t1.follow_id = t0.follow_id
UNION ALL 
SELECT 
      c.full_name AS `c.name`, 
      n AS `n`
FROM test_integration.users_test AS a
INNER JOIN test_integration.user_follows_test AS t0 ON t0.follower_id = a.user_id
INNER JOIN test_integration.user_follows_test AS t1 ON t1.followed_id = t0.followed_id
INNER JOIN test_integration.users_test AS c ON c.user_id = t1.follower_id
LATERAL VIEW explode(array(1, 2)) AS n
WHERE NOT t1.follow_id = t0.follow_id
UNION ALL 
SELECT 
      c.full_name AS `c.name`, 
      n AS `n`
FROM test_integration.users_test AS b
INNER JOIN test_integration.user_follows_test AS t0 ON t0.follower_id = b.user_id
INNER JOIN test_integration.user_follows_test AS t1 ON t1.followed_id = b.user_id
INNER JOIN test_integration.users_test AS c ON c.user_id = t1.follower_id
LATERAL VIEW explode(array(1, 2)) AS n
WHERE NOT t1.follow_id = t0.follow_id
