SELECT 
      a.user_id AS "a.user_id", 
      count(r.follow_id) AS "rel_count"
FROM test_integration.users_test AS a
LEFT JOIN test_integration.user_follows_test AS r ON r.follower_id = a.user_id
GROUP BY a.user_id
