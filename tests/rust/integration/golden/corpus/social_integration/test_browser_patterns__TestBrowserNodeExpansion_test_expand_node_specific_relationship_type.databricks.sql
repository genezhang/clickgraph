SELECT 
      count(*) AS `count`
FROM test_integration.users_test AS n
INNER JOIN test_integration.user_follows_test AS r ON r.follower_id = n.user_id
WHERE n.user_id = 1
