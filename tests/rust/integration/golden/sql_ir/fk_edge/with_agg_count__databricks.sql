WITH with_name_orders_cte_0 AS (SELECT 
      c.name AS `name`, 
      count(o.order_id) AS `orders`
FROM db_fk_edge.orders_fk AS o
INNER JOIN db_fk_edge.customers_fk AS c ON c.customer_id = o.customer_id
GROUP BY c.name
)
SELECT 
      name_orders.name AS `name`, 
      name_orders.orders AS `orders`
FROM with_name_orders_cte_0 AS name_orders
