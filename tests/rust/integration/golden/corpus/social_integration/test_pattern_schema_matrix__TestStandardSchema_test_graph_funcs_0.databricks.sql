SELECT 
      'AUTHORED::User::Post' AS `type(r)`, 
      b.author_id AS `id(a)`, 
      array('User') AS `labels(a)`
FROM test_integration.posts_test AS b
LIMIT 5