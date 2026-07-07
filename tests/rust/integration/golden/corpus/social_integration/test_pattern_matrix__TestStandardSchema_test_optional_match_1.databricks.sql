SELECT 
      a.is_active AS `a.is_active`, 
      count(*) AS `rel_count`
FROM test_integration.users_test AS a
LEFT JOIN test_integration.user_follows_test AS r ON r.follower_id = a.user_id
GROUP BY a.is_active
