SELECT 
      a.name AS "a.name", 
      count(b.user_id) AS "follower_count"
FROM test_integration.users AS b
INNER JOIN test_integration.follows AS t0 ON t0.follower_id = b.user_id
INNER JOIN test_integration.users AS a ON a.user_id = t0.followed_id
GROUP BY a.name
ORDER BY follower_count DESC, a.name ASC
