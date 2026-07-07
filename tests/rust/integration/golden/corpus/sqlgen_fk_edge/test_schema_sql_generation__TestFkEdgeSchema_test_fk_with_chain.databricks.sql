WITH with_c_orders_cte_0 AS (SELECT 
      any_value(c.name) AS `p1_c_name`, 
      count(o.order_id) AS `orders`
FROM db_fk_edge.orders_fk AS o
INNER JOIN db_fk_edge.customers_fk AS c ON c.customer_id = o.customer_id
GROUP BY c.customer_id
HAVING orders > 0
)
SELECT 
      c_orders.p1_c_name AS `c.name`, 
      c_orders.orders AS `orders`
FROM with_c_orders_cte_0 AS c_orders
