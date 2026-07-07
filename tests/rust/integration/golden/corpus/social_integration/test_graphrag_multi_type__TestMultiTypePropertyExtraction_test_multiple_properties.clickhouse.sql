SELECT 
      x.user_id AS "x.user_id", 
      x.full_name AS "x.name", 
      x.email_address AS "x.email", 
      x.city AS "x.city", 
      x.country AS "x.country"
FROM test_integration.users_test AS u
INNER JOIN test_integration.user_follows_test AS t0 ON t0.follower_id = u.user_id
INNER JOIN test_integration.users_test AS x ON x.user_id = t0.followed_id
WHERE u.user_id = 1
LIMIT 5