SELECT `a.name` AS `a.name`, count(`b.user_id`) AS `connections` FROM (
SELECT 
      a.name AS `a.name`,
      b.user_id AS `b.user_id`
FROM test_integration.users AS a
LEFT JOIN test_integration.follows AS t0 ON t0.follower_id = a.user_id
LEFT JOIN test_integration.users AS b ON b.user_id = t0.followed_id
WHERE a.name = 'Bob'
UNION ALL 
SELECT 
      a.name AS `a.name`,
      b.user_id AS `b.user_id`
FROM test_integration.users AS b
LEFT JOIN test_integration.follows AS t0 ON t0.follower_id = b.user_id
LEFT JOIN test_integration.users AS a ON a.user_id = t0.followed_id
WHERE a.name = 'Bob'
) AS __union
GROUP BY `a.name`
