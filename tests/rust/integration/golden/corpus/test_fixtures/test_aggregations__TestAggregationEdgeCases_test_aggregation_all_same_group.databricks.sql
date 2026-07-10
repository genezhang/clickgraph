SELECT 
      'all_users' AS `group_key`, 
      count(n.user_id) AS `total`
FROM test_integration.users AS n
GROUP BY 'all_users'
