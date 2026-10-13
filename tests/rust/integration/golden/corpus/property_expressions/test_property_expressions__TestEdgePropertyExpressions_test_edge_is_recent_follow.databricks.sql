SELECT 
      count(*) AS `recent_follows`
FROM test_integration.users_expressions_test AS v0
JOIN test_integration.follows_expressions_test AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users_expressions_test AS v2 ON v1.followed_id = v2.user_id
WHERE (dateDiff('day', v1.follow_date, today()) <= 7) = true
