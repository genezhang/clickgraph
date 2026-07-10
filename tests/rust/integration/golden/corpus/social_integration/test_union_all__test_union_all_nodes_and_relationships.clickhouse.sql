SELECT DISTINCT 
      'node' AS "entity", 
      n.full_name AS "name"
FROM test_integration.users_test AS n
WHERE n.user_id = 1
UNION ALL 
SELECT DISTINCT 
      'relationship' AS "entity", 
      concat(u1.full_name, ' follows ', u2.full_name) AS "name"
FROM test_integration.users_test AS u1
INNER JOIN test_integration.user_follows_test AS r ON r.follower_id = u1.user_id
INNER JOIN test_integration.users_test AS u2 ON u2.user_id = r.followed_id
WHERE u1.user_id = 1
