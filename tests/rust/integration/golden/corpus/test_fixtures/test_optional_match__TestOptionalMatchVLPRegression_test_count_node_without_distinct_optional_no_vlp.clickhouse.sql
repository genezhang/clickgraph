SELECT 
      a.name AS "a.name", 
      count(t0.followed_id) AS "friend_count"
FROM test_integration.users AS a
LEFT JOIN test_integration.follows AS t0 ON t0.follower_id = a.user_id
WHERE a.name = 'Eve'
GROUP BY a.name
