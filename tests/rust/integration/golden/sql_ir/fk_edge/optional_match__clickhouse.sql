SELECT 
      c.name AS "c.name", 
      o.order_id AS "o.order_id"
FROM db_fk_edge.customers_fk AS c
LEFT JOIN db_fk_edge.orders_fk AS o ON o.customer_id = c.customer_id
LEFT JOIN db_fk_edge.orders_fk AS t0 ON t0.order_id = o.order_id
