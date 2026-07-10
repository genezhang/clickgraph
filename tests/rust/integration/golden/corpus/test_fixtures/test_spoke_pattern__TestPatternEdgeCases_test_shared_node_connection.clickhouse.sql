SELECT 
      a.name AS "a.name", 
      b.name AS "b.name", 
      c.name AS "c.name"
FROM test_integration.users AS a
INNER JOIN test_integration.follows AS t0 ON t0.follower_id = a.user_id
INNER JOIN test_integration.users AS b ON b.user_id = t0.followed_id
INNER JOIN test_integration.follows AS t1 ON t1.follower_id = b.user_id
INNER JOIN test_integration.users AS c ON c.user_id = t1.followed_id
WHERE (a.user_id = 1 AND (t1.follower_id <> t0.follower_id OR t1.followed_id <> t0.followed_id))
