SELECT 
      hub.name AS `hub.name`, 
      a.name AS `a.name`, 
      c.name AS `c.name`
FROM test_integration.users AS hub
INNER JOIN test_integration.follows AS t0 ON t0.follower_id = hub.user_id
INNER JOIN test_integration.users AS a ON a.user_id = t0.followed_id
INNER JOIN test_integration.follows AS t1 ON t1.follower_id = hub.user_id
INNER JOIN test_integration.users AS c ON c.user_id = t1.followed_id
WHERE (hub.user_id = 1 AND (t1.follower_id <> t0.follower_id OR t1.followed_id <> t0.followed_id))
