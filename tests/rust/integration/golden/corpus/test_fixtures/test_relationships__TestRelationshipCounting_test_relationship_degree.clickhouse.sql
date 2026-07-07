SELECT `a.name` AS "a.name", count(DISTINCT `b.user_id`) AS "connections" FROM (
SELECT 
      a.name AS "a.name",
      b.user_id AS "b.user_id"
FROM test_integration.users AS a
INNER JOIN test_integration.follows AS t0 ON t0.follower_id = a.user_id
INNER JOIN test_integration.users AS b ON b.user_id = t0.followed_id
UNION ALL 
SELECT 
      a.name AS "a.name",
      b.user_id AS "b.user_id"
FROM test_integration.users AS b
INNER JOIN test_integration.follows AS t0 ON t0.follower_id = b.user_id
INNER JOIN test_integration.users AS a ON a.user_id = t0.followed_id
) AS __union
GROUP BY `__order_col_1`
ORDER BY connections DESC, a.name ASC
