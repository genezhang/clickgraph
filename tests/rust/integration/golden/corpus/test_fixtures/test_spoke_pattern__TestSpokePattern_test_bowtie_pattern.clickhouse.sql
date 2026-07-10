SELECT 
      a.name AS "a.name", 
      hub.name AS "hub.name", 
      c.name AS "c.name", 
      d.name AS "d.name", 
      e.name AS "e.name"
FROM test_integration.users AS hub
INNER JOIN test_integration.follows AS t0 ON hub.user_id = t0.followed_id
INNER JOIN test_integration.follows AS t1 ON t1.followed_id = hub.user_id
INNER JOIN test_integration.users AS a ON a.user_id = t1.follower_id
INNER JOIN test_integration.follows AS t2 ON t2.follower_id = hub.user_id
INNER JOIN test_integration.users AS c ON c.user_id = t2.followed_id
INNER JOIN test_integration.follows AS t3 ON t3.follower_id = hub.user_id
INNER JOIN test_integration.users AS d ON d.user_id = t3.followed_id
INNER JOIN test_integration.users AS e ON t0.follower_id = e.user_id
WHERE (hub.user_id = 2 AND hub.user_id = 2)
