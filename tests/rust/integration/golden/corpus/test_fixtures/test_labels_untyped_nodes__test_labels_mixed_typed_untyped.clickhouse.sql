SELECT 
      ['TestUser'] AS "user_labels", 
      ['TestUser'] AS "n_labels"
FROM test_integration.users AS n
JOIN test_integration.users AS u ON 1 = 1
WHERE u.user_id = n.user_id
LIMIT 1