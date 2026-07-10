SELECT 
      a.name AS `a.name`, 
      hub.name AS `hub.name`, 
      c.name AS `c.name`
FROM test_integration.users AS hub
INNER JOIN test_integration.follows AS t0 ON hub.user_id = t0.followed_id
INNER JOIN test_integration.follows AS t1 ON t1.followed_id = hub.user_id
INNER JOIN test_integration.users AS a ON a.user_id = t1.follower_id
INNER JOIN test_integration.users AS c ON t0.follower_id = c.user_id
WHERE ((hub.user_id = 2 AND hub.user_id = 2) AND (t0.follower_id <> t1.follower_id OR t0.followed_id <> t1.followed_id))
