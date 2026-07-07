SELECT 
      count(p.post_id) AS `cnt`
FROM test_integration.posts_test AS p
WHERE p.author_id = 1
