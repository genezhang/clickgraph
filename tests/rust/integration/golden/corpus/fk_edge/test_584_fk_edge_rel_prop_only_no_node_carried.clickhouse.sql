WITH with_oid_cte_0 AS (SELECT 
      r.order_id AS "oid"
FROM test_integration.orders_fk AS r
)
SELECT 
      oid.oid AS "oid"
FROM with_oid_cte_0 AS oid
ORDER BY oid.oid ASC
