(SELECT * FROM (
SELECT DISTINCT 
      'node' AS `entity`, 
      n.user_id AS `value`
FROM test_integration.users_test AS n
WHERE n.user_id IS NOT NULL
)
LIMIT 5
)
UNION ALL 
(SELECT * FROM (
SELECT DISTINCT 
      'relationship' AS `entity`, 
      r.follow_date AS `value`
FROM test_integration.user_follows_test AS r
WHERE r.follow_date IS NOT NULL
)
LIMIT 5
)
