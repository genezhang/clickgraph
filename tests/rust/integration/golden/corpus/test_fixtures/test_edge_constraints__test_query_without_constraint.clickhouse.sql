SELECT 
      a.name AS "a.name", 
      b.name AS "b.name"
FROM test_integration.users AS a
INNER JOIN test_integration.friendships AS r ON r.user_id_1 = a.user_id
INNER JOIN test_integration.users AS b ON b.user_id = r.user_id_2
WHERE a.name = 'Alice'
