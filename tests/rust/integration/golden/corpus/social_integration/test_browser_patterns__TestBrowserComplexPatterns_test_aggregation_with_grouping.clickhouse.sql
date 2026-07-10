SELECT 
      u.user_id AS "u.user_id", 
      count(*) AS "follow_count"
FROM test_integration.users_test AS u
INNER JOIN test_integration.user_follows_test AS r ON r.follower_id = u.user_id
GROUP BY u.user_id
ORDER BY follow_count DESC
LIMIT 10