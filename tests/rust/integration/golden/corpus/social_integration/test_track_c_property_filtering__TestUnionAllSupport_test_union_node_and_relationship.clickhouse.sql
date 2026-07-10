SELECT 
      'node' AS "entity", 
      n.user_id AS "value"
FROM test_integration.users_test AS n
WHERE n.user_id = 1
UNION ALL 
SELECT * FROM (
SELECT 
      'relationship' AS "entity", 
      r.follow_date AS "value"
FROM test_integration.user_follows_test AS r
WHERE r.follow_date IS NOT NULL
)
LIMIT 10
