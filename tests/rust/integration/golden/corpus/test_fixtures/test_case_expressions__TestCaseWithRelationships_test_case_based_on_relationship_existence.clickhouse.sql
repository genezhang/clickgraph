WITH optional_o1 AS (
SELECT 
      v1.follower_id AS "v0__user_id", 
      v2.user_id AS "v2__user_id"
FROM test_integration.follows AS v1
JOIN test_integration.users AS v2 ON v1.followed_id = v2.user_id
)
SELECT 
      v0.name AS "n.name", 
      CASE WHEN count(o1.v2__user_id) > 0 THEN 'Active' ELSE 'Inactive' END AS "status"
FROM test_integration.users AS v0
LEFT JOIN optional_o1 AS o1 ON v0.user_id = o1.v0__user_id
GROUP BY v0.name
ORDER BY v0.name ASC
