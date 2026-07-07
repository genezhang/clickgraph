SELECT 
      count(*) AS `total`
FROM test_integration.posts_test AS b
WHERE b.author_id > 0
