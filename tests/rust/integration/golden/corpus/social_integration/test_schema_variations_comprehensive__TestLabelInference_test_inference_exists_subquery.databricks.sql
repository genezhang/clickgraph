SELECT 
      count(*) AS `total`
FROM test_integration.users_test AS a
WHERE EXISTS (SELECT 1 FROM test_integration.posts_test WHERE posts_test.author_id = a.user_id)
