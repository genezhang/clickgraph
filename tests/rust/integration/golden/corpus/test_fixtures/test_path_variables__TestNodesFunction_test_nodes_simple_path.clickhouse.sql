SELECT 
      array(a.user_id, b.user_id) AS "path_nodes"
FROM test_integration.users AS a
INNER JOIN test_integration.follows AS t0 ON t0.follower_id = a.user_id
INNER JOIN test_integration.users AS b ON b.user_id = t0.followed_id
WHERE (a.name = 'Alice' AND b.name = 'Bob')
