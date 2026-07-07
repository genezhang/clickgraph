WITH with_c_cte_0 AS (SELECT 
      c.name AS `p1_c_name`
FROM db_fk_edge.customers_fk AS c
WHERE c.customer_id > 100
)
SELECT 
      c.p1_c_name AS `c.name`, 
      o.order_id AS `o.order_id`
FROM db_fk_edge.orders_fk AS o
INNER JOIN with_c_cte_0 AS c ON 1 = 1
