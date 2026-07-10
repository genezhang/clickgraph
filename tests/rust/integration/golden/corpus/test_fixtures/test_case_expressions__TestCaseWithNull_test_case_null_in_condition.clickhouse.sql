SELECT 
      a.name AS "a.name", 
      CASE WHEN b.name IS NULL THEN 'No connections' ELSE 'Has connections' END AS "connection_status"
FROM test_integration.users AS a
LEFT JOIN test_integration.follows AS t0 ON t0.follower_id = a.user_id
LEFT JOIN test_integration.users AS b ON b.user_id = t0.followed_id
ORDER BY a.name ASC
