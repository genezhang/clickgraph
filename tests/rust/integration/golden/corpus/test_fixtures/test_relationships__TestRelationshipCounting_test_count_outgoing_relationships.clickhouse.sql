SELECT 
      a.name AS "a.name", 
      count(t0.followed_id) AS "following_count"
FROM test_integration.users AS a
INNER JOIN test_integration.follows AS t0 ON t0.follower_id = a.user_id
GROUP BY a.name
ORDER BY following_count DESC, a.name ASC
