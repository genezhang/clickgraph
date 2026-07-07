SELECT 
      a.name AS "a.name", 
      b.name AS "b.name", 
      CASE WHEN r.since > 2022 THEN 'Recent' WHEN r.since > 2020 THEN 'Medium' ELSE 'Old' END AS "relationship_age"
FROM test_integration.users AS a
INNER JOIN test_integration.follows AS r ON r.follower_id = a.user_id
INNER JOIN test_integration.users AS b ON b.user_id = r.followed_id
ORDER BY a.name ASC, b.name ASC
