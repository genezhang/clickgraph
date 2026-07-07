SELECT `b.name` AS `b.name` FROM (
SELECT DISTINCT 
      b.name AS "b.name", 
      b.name AS "__order_col_0"
FROM test_integration.users AS a
INNER JOIN test_integration.follows AS r1 ON a.user_id = r1.follower_id
INNER JOIN test_integration.follows AS r2 ON r1.followed_id = r2.follower_id
INNER JOIN test_integration.users AS b ON r2.followed_id = b.user_id
WHERE a.name = 'Alice'
UNION DISTINCT 
SELECT DISTINCT 
      b.name AS "b.name", 
      b.name AS "__order_col_0"
FROM test_integration.users AS a
INNER JOIN test_integration.follows AS r2 ON r2.followed_id = a.user_id
INNER JOIN test_integration.follows AS r1 ON r1.followed_id = r2.follower_id
INNER JOIN test_integration.users AS b ON b.user_id = r1.follower_id
WHERE a.name = 'Alice'
) AS __union
ORDER BY __union.`__order_col_0` ASC
