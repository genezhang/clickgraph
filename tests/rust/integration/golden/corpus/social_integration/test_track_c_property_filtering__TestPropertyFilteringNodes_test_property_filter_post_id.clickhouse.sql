WITH v0_labels AS (
SELECT 
      'Post' AS "__cg_label", 
      e.post_id AS "__cg_id_0", 
      e.post_id AS "p2_v0_post_id"
FROM test_integration.posts_test AS e

UNION ALL
SELECT 
      'User' AS "__cg_label", 
      e.user_id AS "__cg_id_0", 
      NULL AS "p2_v0_post_id"
FROM test_integration.users_test AS e
)
SELECT 
      v0.p2_v0_post_id AS "pid"
FROM v0_labels AS v0
WHERE v0.p2_v0_post_id = 1
