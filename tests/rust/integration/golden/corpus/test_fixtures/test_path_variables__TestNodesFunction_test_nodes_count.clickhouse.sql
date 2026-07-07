SELECT 
      length(nodes(p)) AS "node_count"
FROM test_integration.users AS a
INNER JOIN test_integration.follows AS t0 ON t0.follower_id = a.user_id
INNER JOIN test_integration.follows AS t1 ON t1.follower_id = t0.followed_id
WHERE a.name = 'Alice'
