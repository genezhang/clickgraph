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
LEFT JOIN (SELECT t0.follower_id AS __cg_combined_anchor_key, a.* FROM test_integration.follows AS t0 JOIN test_integration.users AS a ON a.user_id = t0.followed_id WHERE name = 'Bob') AS a ON a.__cg_combined_anchor_key = b.user_id
) AS __union
GROUP BY `a.name`
