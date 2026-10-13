SELECT 
      'FOLLOWS' AS "type(r)", 
      count(*) AS "cnt"
FROM test_integration.users_test AS v0
JOIN test_integration.user_follows_test AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.followed_id = v2.user_id
HAVING count(*) > 0

-- column type(r): Value [("type(r)", "type(r)")]
-- column cnt: Value [("cnt", "cnt")]