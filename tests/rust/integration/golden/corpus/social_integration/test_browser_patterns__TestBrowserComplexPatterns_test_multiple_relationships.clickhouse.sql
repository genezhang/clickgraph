SELECT 
      count(*) AS "count"
FROM test_integration.posts_test AS p
INNER JOIN test_integration.user_follows_test AS r2 ON r2.follower_id = p.author_id
