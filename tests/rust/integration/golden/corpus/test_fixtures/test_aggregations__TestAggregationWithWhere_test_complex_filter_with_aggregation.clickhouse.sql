SELECT 
      a.name AS "a.name", 
      count(b.user_id) AS "mature_follows"
FROM test_integration.users AS a
INNER JOIN test_integration.follows AS t0 ON t0.follower_id = a.user_id
INNER JOIN test_integration.users AS b ON b.user_id = t0.followed_id
WHERE (a.age > 25 AND b.age > 25)
GROUP BY a.name
ORDER BY a.name ASC
