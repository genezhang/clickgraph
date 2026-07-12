SELECT 
      a.name AS `a.name`, 
      b.name AS `b.name`, 
      b.age AS `b.age`
FROM test_integration.users AS a
LEFT JOIN (SELECT t0.follower_id AS __cg_combined_anchor_key, b.* FROM test_integration.follows AS t0 JOIN test_integration.users AS b ON b.user_id = t0.followed_id WHERE b.age > 25) AS b ON b.__cg_combined_anchor_key = a.user_id
WHERE a.name = 'Alice'
ORDER BY b.name ASC
