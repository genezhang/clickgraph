SELECT 
      count(*) AS "count"
FROM test_integration.user_follows_test AS r
INNER JOIN test_integration.users_test AS m ON r.follower_id = m.user_id
WHERE r.followed_id = 1
