SELECT 
      count(r2.followed_id) AS "cnt"
FROM test_integration.users_test AS u
INNER JOIN test_integration.user_follows_test AS r1 ON u.user_id = r1.follower_id
INNER JOIN test_integration.user_follows_test AS r2 ON r1.followed_id = r2.follower_id
WHERE (u.user_id = 1 AND NOT (r1.follower_id = r2.follower_id AND r1.followed_id = r2.followed_id))
