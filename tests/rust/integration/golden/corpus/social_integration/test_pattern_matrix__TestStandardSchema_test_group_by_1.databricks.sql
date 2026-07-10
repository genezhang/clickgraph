SELECT 
      n.email_address AS `n.email`, 
      count(n.user_id) AS `cnt`
FROM test_integration.users_test AS n
GROUP BY n.email_address
ORDER BY cnt DESC
LIMIT 10