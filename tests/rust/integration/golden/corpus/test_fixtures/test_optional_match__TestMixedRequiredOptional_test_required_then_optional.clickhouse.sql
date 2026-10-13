WITH optional_o1 AS (
SELECT 
      v3.follower_id AS "v2__user_id", 
      v4.user_id AS "v4__user_id", 
      v4.name AS "p2_v4_name"
FROM test_integration.follows AS v3
JOIN test_integration.users AS v4 ON v3.followed_id = v4.user_id
)
SELECT 
      v0.name AS "a.name", 
      v2.name AS "b.name", 
      o1.p2_v4_name AS "c.name"
FROM test_integration.users AS v0
JOIN test_integration.follows AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users AS v2 ON v1.followed_id = v2.user_id
LEFT JOIN optional_o1 AS o1 ON v2.user_id = o1.v2__user_id
ORDER BY v0.name ASC, v2.name ASC, o1.p2_v4_name ASC

-- column a.name: Value [("a.name", "a.name")]
-- column b.name: Value [("b.name", "b.name")]
-- column c.name: Value [("c.name", "c.name")]