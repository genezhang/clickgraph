WITH with_o_r_cte_0 AS (SELECT 
      o.order_id AS "p1_o_order_id"
FROM test_integration.orders_fk AS o
WHERE o.customer_id > 2
)
SELECT 
      o_r.p1_o_order_id AS "o.order_id"
FROM with_o_r_cte_0 AS o_r
ORDER BY o_r.p1_o_order_id ASC
