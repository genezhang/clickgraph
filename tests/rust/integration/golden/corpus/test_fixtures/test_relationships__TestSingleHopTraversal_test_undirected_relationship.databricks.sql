SELECT `b.name` AS `b.name` FROM (
SELECT 
      b.name AS `b.name`, 
      b.name AS `__order_col_0`
FROM test_integration.users AS a
INNER JOIN test_integration.follows AS r ON r.follower_id = a.user_id
INNER JOIN test_integration.users AS b ON b.user_id = r.followed_id
WHERE a.name = 'Bob'
UNION ALL 
SELECT 
      b.name AS `b.name`, 
      b.name AS `__order_col_0`
FROM test_integration.users AS a
INNER JOIN test_integration.follows AS r ON a.user_id = r.followed_id
INNER JOIN test_integration.users AS b ON r.follower_id = b.user_id
WHERE a.name = 'Bob'
) AS __union
ORDER BY __union.`__order_col_0` ASC
