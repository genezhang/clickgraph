SELECT 
      a.name AS "a.name", 
      b.name AS "b.name", 
      c.name AS "c.name"
FROM test_integration.users AS b
INNER JOIN test_integration.follows AS t0 ON b.user_id = t0.followed_id
INNER JOIN test_integration.follows AS t1 ON t1.followed_id = b.user_id
INNER JOIN test_integration.users AS a ON a.user_id = t1.follower_id
INNER JOIN test_integration.users AS c ON t0.follower_id = c.user_id
WHERE (b.user_id = 2 AND b.user_id = 2)
