SELECT 
      v0.post_id AS "n.post_id", 
      count(v0.post_id) AS "cnt"
FROM test_integration.posts_test AS v0
GROUP BY v0.post_id
ORDER BY count(v0.post_id) DESC NULLS FIRST
LIMIT 10