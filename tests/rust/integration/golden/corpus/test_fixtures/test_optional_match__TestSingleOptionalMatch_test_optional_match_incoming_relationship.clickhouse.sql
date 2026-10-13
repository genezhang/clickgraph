WITH optional_o1 AS (
SELECT 
      v0.user_id AS "v0__user_id", 
      v1.user_id AS "v1__user_id", 
      v1.name AS "p2_v1_name"
FROM test_integration.users AS v1
JOIN test_integration.follows AS v2 ON v2.follower_id = v1.user_id
JOIN test_integration.users AS v0 ON v2.followed_id = v0.user_id
WHERE v0.name = 'Alice'
)
SELECT 
      v0.name AS "a.name", 
      o1.p2_v1_name AS "b.name"
FROM test_integration.users AS v0
LEFT JOIN optional_o1 AS o1 ON v0.user_id = o1.v0__user_id
WHERE v0.name = 'Alice'

-- column a.name: Value [("a.name", "a.name")]
-- column b.name: Value [("b.name", "b.name")]