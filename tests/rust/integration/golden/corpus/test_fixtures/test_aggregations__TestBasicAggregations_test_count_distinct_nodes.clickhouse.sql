SELECT 
      count(DISTINCT a.user_id) AS "unique_followers"
FROM test_integration.users AS a
INNER JOIN test_integration.follows AS t0 ON t0.follower_id = a.user_id
