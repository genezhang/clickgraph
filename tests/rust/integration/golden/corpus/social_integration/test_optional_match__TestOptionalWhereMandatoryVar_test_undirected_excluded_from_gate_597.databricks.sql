SELECT 
      a.user_id AS `a.user_id`, 
      t0.followed_id AS `b.user_id`
FROM test_integration.users_test AS a
LEFT JOIN (SELECT e.follower_id, e.followed_id, e.follow_date, e.follow_id FROM test_integration.user_follows_test AS e UNION ALL SELECT e.followed_id AS follower_id, e.follower_id AS followed_id, e.follow_date, e.follow_id FROM test_integration.user_follows_test AS e) AS t0 ON t0.follower_id = a.user_id
WHERE a.is_active = true
ORDER BY a.user_id ASC NULLS LAST, t0.followed_id ASC NULLS LAST
