SELECT 
      percentile(v2.user_id, 0.9) AS `percentileCont(b.user_id, 0.9)`
FROM test_integration.users_test AS v0
JOIN test_integration.user_follows_test AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.followed_id = v2.user_id

-- column percentileCont(b.user_id, 0.9): Value [("percentileCont(b.user_id, 0.9)", "percentileCont(b.user_id, 0.9)")]