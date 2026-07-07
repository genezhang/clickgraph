SELECT 
      count(*) AS "total"
FROM test_integration.posts_test AS p
INNER JOIN test_integration.user_follows_test AS t0 ON t0.follower_id = p.author_id
