SELECT 
      u1.user_id AS "u1.user_id", 
      f.followed_id AS "u2.user_id", 
      dateDiff('day', f.follow_date, today()) AS "f.follow_age_days"
FROM test_integration.follows_expressions_test AS f
INNER JOIN test_integration.users_expressions_test AS u1 ON f.follower_id = u1.user_id
WHERE dateDiff('day', f.follow_date, today()) < 10
ORDER BY dateDiff('day', f.follow_date, today()) ASC
