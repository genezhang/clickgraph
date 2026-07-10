SELECT 
      'FOLLOWS' AS `type(r)`, 
      a.user_id AS `id(a)`, 
      array('User') AS `labels(a)`
FROM test_integration.users_test AS a
INNER JOIN test_integration.user_follows_test AS r ON r.follower_id = a.user_id
LIMIT 5