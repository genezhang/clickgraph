SELECT 
      c.customer_id AS "c.customer_id", 
      o.order_id AS "o.order_id"
FROM db_fk_edge.customers_fk AS c
LEFT JOIN (SELECT * FROM db_fk_edge.orders_fk WHERE total_amount > 100) AS o ON o.customer_id = c.customer_id AND c.customer_id > 101
