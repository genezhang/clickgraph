SELECT 
      v0.post_content AS `a.content`, 
      CASE WHEN count(*) >= 0 THEN 0 ELSE 0 END AS `rel_count`
FROM test_integration.posts_test AS v0
GROUP BY v0.post_content

-- column a.content: Value [("a.content", "a.content")]
-- column rel_count: Value [("rel_count", "rel_count")]