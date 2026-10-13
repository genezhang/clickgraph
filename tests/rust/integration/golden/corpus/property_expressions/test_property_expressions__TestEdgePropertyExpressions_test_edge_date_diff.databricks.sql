SELECT 
      v0.user_id AS `u1.user_id`, 
      v2.user_id AS `u2.user_id`, 
      dateDiff('day', v1.follow_date, today()) AS `f.follow_age_days`
FROM test_integration.users_expressions_test AS v0
JOIN test_integration.follows_expressions_test AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users_expressions_test AS v2 ON v1.followed_id = v2.user_id
WHERE dateDiff('day', v1.follow_date, today()) < 10
ORDER BY dateDiff('day', v1.follow_date, today()) ASC NULLS LAST

-- column u1.user_id: Value [("u1.user_id", "u1.user_id")]
-- column u2.user_id: Value [("u2.user_id", "u2.user_id")]
-- column f.follow_age_days: Value [("f.follow_age_days", "f.follow_age_days")]