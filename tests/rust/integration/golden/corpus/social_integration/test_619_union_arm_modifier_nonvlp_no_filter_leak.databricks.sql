(SELECT * FROM (
SELECT 
      t0.followed_id AS `id`
FROM test_integration.users_test AS a
INNER JOIN test_integration.user_follows_test AS t0 ON t0.follower_id = a.user_id
WHERE a.user_id = 5
)
ORDER BY id ASC
LIMIT 2
)
UNION ALL 
SELECT 
      a.user_id AS `id`
FROM test_integration.users_test AS a
WHERE a.user_id = 6
