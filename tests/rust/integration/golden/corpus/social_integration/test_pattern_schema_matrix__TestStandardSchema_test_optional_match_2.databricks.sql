SELECT 
      v0.post_id AS `a.post_id`, 
      CASE WHEN count(*) >= 0 THEN 0 ELSE 0 END AS `rel_count`
FROM test_integration.posts_test AS v0
GROUP BY v0.post_id
