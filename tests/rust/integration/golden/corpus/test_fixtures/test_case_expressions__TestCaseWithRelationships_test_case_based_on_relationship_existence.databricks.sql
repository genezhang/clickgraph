SELECT 
      n.name AS `n.name`, 
      CASE WHEN count(t0.followed_id) > 0 THEN 'Active' ELSE 'Inactive' END AS `status`
FROM test_integration.users AS n
LEFT JOIN test_integration.follows AS t0 ON t0.follower_id = n.user_id
GROUP BY n.name
ORDER BY n.name ASC
