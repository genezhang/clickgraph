SELECT 
      n.post_id AS `n.post_id`
FROM test_integration.posts_test AS n
WHERE n.post_id IS NOT NULL
ORDER BY n.post_id DESC
LIMIT 10 OFFSET 5