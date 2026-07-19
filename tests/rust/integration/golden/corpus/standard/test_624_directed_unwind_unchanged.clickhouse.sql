SELECT 
      b.full_name AS "b.name", 
      n AS "n"
FROM test_integration.users_test AS a
INNER JOIN test_integration.user_follows_test AS t0 ON t0.follower_id = a.user_id
INNER JOIN test_integration.users_test AS b ON b.user_id = t0.followed_id
ARRAY JOIN [1, 2] AS n
