SELECT 
      c.customer_id AS "c.customer_id", 
      o.order_id AS "o.order_id"
FROM db_fk_edge.customers_fk AS c
LEFT JOIN db_fk_edge.orders_fk AS o ON o.customer_id = c.customer_id
LEFT JOIN (SELECT * FROM db_fk_edge.orders_fk WHERE order_date > '2024-01-01') AS r ON r.order_id = o.order_id
