SELECT 
      a.name AS "a.name", 
      b.name AS "b.name", 
      count(*) AS "connection_count"
FROM test_integration.users AS a
INNER JOIN test_integration.follows AS t0 ON t0.follower_id = a.user_id
INNER JOIN test_integration.users AS b ON b.user_id = t0.followed_id
GROUP BY a.name, b.name
ORDER BY a.name ASC, b.name ASC
