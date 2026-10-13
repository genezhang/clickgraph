WITH v0_labels AS (
SELECT 
      'Post' AS `__cg_label`, 
      e.post_id AS `__cg_id_0`
FROM test_integration.posts_test AS e

UNION ALL
SELECT 
      'User' AS `__cg_label`, 
      e.user_id AS `__cg_id_0`
FROM test_integration.users_test AS e
)
SELECT 
      count(v0.__cg_label) AS `total`
FROM v0_labels AS v0

-- column total: Value [("total", "total")]