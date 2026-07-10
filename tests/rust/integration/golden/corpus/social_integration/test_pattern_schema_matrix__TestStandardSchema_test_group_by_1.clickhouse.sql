SELECT 
      n.user_id AS "n.user_id", 
      count(n.user_id) AS "cnt"
FROM test_integration.users_test AS n
GROUP BY n.user_id
ORDER BY cnt DESC
LIMIT 10