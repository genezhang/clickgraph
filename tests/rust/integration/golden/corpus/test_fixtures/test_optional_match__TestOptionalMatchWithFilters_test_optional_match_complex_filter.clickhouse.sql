SELECT 
      a.name AS "a.name", 
      count(b.user_id) AS "filtered_follows"
FROM test_integration.users AS a
LEFT JOIN test_integration.follows AS t0 ON t0.follower_id = a.user_id
LEFT JOIN test_integration.users AS b ON b.user_id = t0.followed_id
WHERE (b.age > 25 AND b.name <> 'Charlie')
GROUP BY a.name
ORDER BY a.name ASC
