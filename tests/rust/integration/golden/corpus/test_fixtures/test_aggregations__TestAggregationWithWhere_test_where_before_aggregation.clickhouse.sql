SELECT 
      a.name AS "a.name", 
      count(b.user_id) AS "follows_older"
FROM test_integration.users AS b
INNER JOIN test_integration.follows AS t0 ON b.user_id = t0.followed_id
INNER JOIN test_integration.users AS a ON t0.follower_id = a.user_id
WHERE b.age > 25
GROUP BY a.name
ORDER BY a.name ASC
