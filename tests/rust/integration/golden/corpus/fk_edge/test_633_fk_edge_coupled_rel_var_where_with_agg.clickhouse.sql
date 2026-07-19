WITH with_o_rc_cte_0 AS (SELECT 
      o.order_id AS "p1_o_order_id", 
      count(o.order_id) AS "rc"
FROM test_integration.orders_fk AS o
WHERE o.customer_id > 2
GROUP BY o.order_id
)
SELECT 
      o_rc.p1_o_order_id AS "o.order_id", 
      o_rc.rc AS "rc"
FROM with_o_rc_cte_0 AS o_rc
ORDER BY o_rc.p1_o_order_id ASC
