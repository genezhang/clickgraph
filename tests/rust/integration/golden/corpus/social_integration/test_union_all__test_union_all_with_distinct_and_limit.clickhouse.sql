SELECT * FROM (
SELECT DISTINCT 
      'node' AS "entity", 
      n.full_name AS "name"
FROM test_integration.users_test AS n
WHERE n.user_id < 5
)
LIMIT 2
UNION ALL 
SELECT * FROM (
SELECT DISTINCT 
      'post' AS "entity", 
      p.post_content AS "name"
FROM test_integration.posts_test AS p
WHERE p.post_id < 5
)
LIMIT 2
