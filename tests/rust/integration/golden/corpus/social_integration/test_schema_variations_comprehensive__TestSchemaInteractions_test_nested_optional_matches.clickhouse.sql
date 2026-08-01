SELECT 
      count(*) AS "total"
FROM test_integration.users_test AS a
LEFT JOIN test_integration.posts_test AS p ON p.author_id = a.user_id
LEFT JOIN test_integration.post_likes_test AS t0 ON t0.post_id = p.post_id
