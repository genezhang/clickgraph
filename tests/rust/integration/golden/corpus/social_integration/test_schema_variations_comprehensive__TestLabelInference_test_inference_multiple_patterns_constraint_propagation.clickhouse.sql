SELECT 
      count(*) AS "total"
FROM test_integration.users_test AS a
INNER JOIN test_integration.user_follows_test AS t0 ON t0.follower_id = a.user_id
INNER JOIN test_integration.posts_test AS c ON c.author_id = t0.followed_id
