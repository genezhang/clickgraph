SELECT 
      'all_users' AS `group_key`, 
      count(v0.user_id) AS `total`
FROM test_integration.users AS v0
HAVING count(*) > 0

-- column group_key: Value [("group_key", "group_key")]
-- column total: Value [("total", "total")]