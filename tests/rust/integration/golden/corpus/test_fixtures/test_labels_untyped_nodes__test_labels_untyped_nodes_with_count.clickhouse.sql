WITH v0_labels AS (
SELECT 
      'TestProduct' AS "__cg_label", 
      e.product_id AS "__cg_id_0"
FROM test_integration.products AS e

UNION ALL
SELECT 
      'TestUser' AS "__cg_label", 
      e.user_id AS "__cg_id_0"
FROM test_integration.users AS e
)
SELECT 
      [v0.__cg_label] AS "label", 
      count(*) AS "cnt"
FROM v0_labels AS v0
GROUP BY [v0.__cg_label]
ORDER BY [v0.__cg_label] ASC

-- column label: Value [("label", "label")]
-- column cnt: Value [("cnt", "cnt")]