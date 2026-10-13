SELECT 
      v0.post_id AS `a.post_id`, 
      CASE WHEN count(*) >= 0 THEN 0 ELSE 0 END AS `rel_count`
FROM test_integration.posts_test AS v0
GROUP BY v0.post_id

-- column a.post_id: Value [("a.post_id", "a.post_id")]
-- column rel_count: Value [("rel_count", "rel_count")]