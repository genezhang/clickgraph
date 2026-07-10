SELECT 
      n.post_id AS "n.post_id", 
      count(n.post_id) AS "cnt"
FROM test_integration.posts_test AS n
GROUP BY n.post_id
ORDER BY cnt DESC
LIMIT 10