WITH v1_labels AS (
SELECT 
      'TestProduct' AS "__cg_label", 
      e.product_id AS "__cg_id_0", 
      NULL AS "p2_v1_user_id"
FROM test_integration.products AS e

UNION ALL
SELECT 
      'TestUser' AS "__cg_label", 
      e.user_id AS "__cg_id_0", 
      e.user_id AS "p2_v1_user_id"
FROM test_integration.users AS e
)
SELECT 
      ['TestUser'] AS "user_labels", 
      [v1.__cg_label] AS "n_labels"
FROM test_integration.users AS v0
CROSS JOIN v1_labels AS v1
WHERE v0.user_id = v1.p2_v1_user_id
LIMIT 1