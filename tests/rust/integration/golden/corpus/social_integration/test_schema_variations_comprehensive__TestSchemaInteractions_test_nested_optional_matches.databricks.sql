SELECT 
      count(*) AS `total`
FROM test_integration.posts_test AS p
LEFT JOIN test_integration.post_likes_test AS t0 ON t0.post_id = p.post_id
