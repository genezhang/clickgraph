SELECT 
      a.name AS `a.name`
FROM test_integration.users AS a
INNER JOIN test_integration.follows AS t0 ON t0.follower_id = a.user_id
INNER JOIN test_integration.follows AS t1 ON t1.follower_id = t0.followed_id
INNER JOIN test_integration.follows AS t2 ON t2.follower_id = t1.followed_id
INNER JOIN test_integration.follows AS t3 ON t3.follower_id = t2.followed_id
INNER JOIN test_integration.follows AS t4 ON t4.follower_id = t3.followed_id
INNER JOIN test_integration.follows AS t5 ON t5.follower_id = t4.followed_id
WHERE (((((((((((((((t5.follower_id <> t4.follower_id OR t5.followed_id <> t4.followed_id) AND (t5.follower_id <> t3.follower_id OR t5.followed_id <> t3.followed_id)) AND (t5.follower_id <> t2.follower_id OR t5.followed_id <> t2.followed_id)) AND (t5.follower_id <> t1.follower_id OR t5.followed_id <> t1.followed_id)) AND (t5.follower_id <> t0.follower_id OR t5.followed_id <> t0.followed_id)) AND (t4.follower_id <> t3.follower_id OR t4.followed_id <> t3.followed_id)) AND (t4.follower_id <> t2.follower_id OR t4.followed_id <> t2.followed_id)) AND (t4.follower_id <> t1.follower_id OR t4.followed_id <> t1.followed_id)) AND (t4.follower_id <> t0.follower_id OR t4.followed_id <> t0.followed_id)) AND (t3.follower_id <> t2.follower_id OR t3.followed_id <> t2.followed_id)) AND (t3.follower_id <> t1.follower_id OR t3.followed_id <> t1.followed_id)) AND (t3.follower_id <> t0.follower_id OR t3.followed_id <> t0.followed_id)) AND (t2.follower_id <> t1.follower_id OR t2.followed_id <> t1.followed_id)) AND (t2.follower_id <> t0.follower_id OR t2.followed_id <> t0.followed_id)) AND (t1.follower_id <> t0.follower_id OR t1.followed_id <> t0.followed_id))
