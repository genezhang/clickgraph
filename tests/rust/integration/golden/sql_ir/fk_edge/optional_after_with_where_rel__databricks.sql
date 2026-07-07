WITH with_c_cte_0 AS (SELECT 
      c.customer_id AS `p1_c_customer_id`
FROM db_fk_edge.customers_fk AS c
)
SELECT 
      c.p1_c_customer_id AS `c.customer_id`, 
      o.order_id AS `o.order_id`
FROM with_c_cte_0 AS c
LEFT JOIN (SELECT * FROM db_fk_edge.orders_fk WHERE order_id > 3) AS o ON o.customer_id = c.p1_c_customer_id
