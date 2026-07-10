SELECT 
      a.name AS `a.name`, 
      caseWithExpression(b.name, NULL, 'No follow', b.name) AS `followed`
FROM test_integration.users AS a
LEFT JOIN test_integration.follows AS t0 ON t0.follower_id = a.user_id
LEFT JOIN test_integration.users AS b ON b.user_id = t0.followed_id
ORDER BY a.name ASC, followed ASC
