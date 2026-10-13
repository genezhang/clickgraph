WITH v0_labels AS (
SELECT 
      'Post' AS `__cg_label`, 
      e.post_id AS `__cg_id_0`, 
      NULL AS `p2_v0_email`, 
      NULL AS `p2_v0_user_id`
FROM test_integration.posts_test AS e

UNION ALL
SELECT 
      'User' AS `__cg_label`, 
      e.user_id AS `__cg_id_0`, 
      e.email_address AS `p2_v0_email`, 
      e.user_id AS `p2_v0_user_id`
FROM test_integration.users_test AS e
)
SELECT 
      v0.p2_v0_user_id AS `uid`, 
      v0.p2_v0_email AS `email`
FROM v0_labels AS v0
WHERE (v0.p2_v0_user_id = 1 AND v0.p2_v0_email IS NOT NULL)

-- column uid: Value [("uid", "uid")]
-- column email: Value [("email", "email")]