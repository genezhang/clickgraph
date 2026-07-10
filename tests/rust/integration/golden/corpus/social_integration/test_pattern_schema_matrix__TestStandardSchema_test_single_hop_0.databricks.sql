SELECT 
      a.full_name AS `a.name`, 
      r.post_id AS `b.post_id`
FROM test_integration.users_test AS a
INNER JOIN test_integration.post_likes_test AS r ON r.user_id = a.user_id
LIMIT 10