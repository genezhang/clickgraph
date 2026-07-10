SELECT 
      u.user_id AS "u.user_id", 
      count(*) AS "follow_count"
FROM test_integration.users_test AS u
LEFT JOIN test_integration.user_follows_test AS r ON r.follower_id = u.user_id
GROUP BY u.user_id
LIMIT 5