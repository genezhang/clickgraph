SELECT DISTINCT 
      t0.followed_id AS `fof.user_id`
FROM test_integration.users AS user
INNER JOIN test_integration.follows AS t1 ON t1.follower_id = user.user_id
INNER JOIN test_integration.follows AS t0 ON t0.follower_id = t1.followed_id
WHERE user.user_id = 1
ORDER BY `fof.user_id` ASC
