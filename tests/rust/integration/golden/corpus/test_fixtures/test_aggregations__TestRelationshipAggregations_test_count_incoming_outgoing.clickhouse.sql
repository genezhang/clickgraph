SELECT 
      n.name AS "n.name", 
      count(DISTINCT t0.followed_id) AS "following", 
      count(DISTINCT t1.follower_id) AS "followers"
FROM test_integration.users AS n
LEFT JOIN test_integration.follows AS t0 ON t0.follower_id = n.user_id
LEFT JOIN test_integration.follows AS t1 ON t1.followed_id = n.user_id
GROUP BY n.name
ORDER BY n.name ASC
