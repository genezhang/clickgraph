SELECT 
      t0.followed_id AS `u2.user_id`, 
      p.post_id AS `p.post_id`
FROM test_integration.users_test AS u1
INNER JOIN test_integration.user_follows_test AS t0 ON t0.follower_id = u1.user_id
INNER JOIN test_integration.posts_test AS p ON p.author_id = t0.followed_id
