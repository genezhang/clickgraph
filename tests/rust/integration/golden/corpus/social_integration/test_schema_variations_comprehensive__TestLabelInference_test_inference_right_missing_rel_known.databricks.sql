SELECT 
      count(*) AS `total`
FROM test_integration.users_test AS v0
JOIN test_integration.posts_test AS v1 ON v1.author_id = v0.user_id
JOIN test_integration.posts_test AS v2 ON v1.post_id = v2.post_id
WHERE v2.post_id > 0

-- column total: Value [("total", "total")]