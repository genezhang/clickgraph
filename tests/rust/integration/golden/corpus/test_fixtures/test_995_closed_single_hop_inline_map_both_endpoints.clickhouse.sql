SELECT 
      a.user_id AS "a.user_id"
FROM test_integration.users AS a
INNER JOIN test_integration.follows AS t0 ON t0.follower_id = a.user_id AND t0.followed_id = a.user_id
WHERE ((a.name = 'Alice' AND a.user_id = 1) AND t0.follower_id = t0.followed_id)
