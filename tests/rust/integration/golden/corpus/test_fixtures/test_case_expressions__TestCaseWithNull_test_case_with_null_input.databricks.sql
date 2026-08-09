SELECT 
      a.name AS `a.name`, 
      CASE b.name WHEN NULL THEN 'No follow' ELSE b.name END AS `followed`
FROM test_integration.users AS a
LEFT JOIN test_integration.follows AS t0 ON t0.follower_id = a.user_id
LEFT JOIN test_integration.users AS b ON b.user_id = t0.followed_id
ORDER BY a.name ASC NULLS LAST, followed ASC NULLS LAST
