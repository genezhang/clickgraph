SELECT 
      hub.name AS `hub.name`, 
      count(follower.user_id) AS `follower_count`
FROM test_integration.users AS hub
INNER JOIN test_integration.follows AS t0 ON hub.user_id = t0.followed_id
INNER JOIN test_integration.users AS follower ON t0.follower_id = follower.user_id
WHERE hub.user_id = 2
GROUP BY hub.name
