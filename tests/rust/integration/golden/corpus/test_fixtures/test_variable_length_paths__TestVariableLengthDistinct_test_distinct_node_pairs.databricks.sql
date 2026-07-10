SELECT DISTINCT 
      a.name AS `a.name`, 
      b.name AS `b.name`
FROM test_integration.users AS a
INNER JOIN test_integration.follows AS r1 ON a.user_id = r1.follower_id
INNER JOIN test_integration.follows AS r2 ON r1.followed_id = r2.follower_id
INNER JOIN test_integration.users AS b ON r2.followed_id = b.user_id
WHERE a.user_id <> b.user_id
ORDER BY `a.name` ASC, `b.name` ASC
