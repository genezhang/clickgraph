SELECT 
      c.customer_id AS "c.customer_id", 
      o.order_id AS "o.order_id"
FROM db_fk_edge.customers_fk AS c
LEFT JOIN db_fk_edge.orders_fk AS o ON o.customer_id = c.customer_id
WHERE c.customer_id > 101
