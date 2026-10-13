WITH optional_o1 AS (
SELECT 
      v1.follower_id AS "v0__user_id", 
      v2.user_id AS "v2__user_id", 
      v2.name AS "p2_v2_name"
FROM test_integration.follows AS v1
JOIN test_integration.users AS v2 ON v1.followed_id = v2.user_id
)
SELECT 
      v0.name AS "a.name", 
      o1.p2_v2_name AS "followed"
FROM test_integration.users AS v0
LEFT JOIN optional_o1 AS o1 ON v0.user_id = o1.v0__user_id
ORDER BY v0.name ASC, o1.p2_v2_name ASC

-- column a.name: Value [("a.name", "a.name")]
-- column followed: Value [("followed", "followed")]