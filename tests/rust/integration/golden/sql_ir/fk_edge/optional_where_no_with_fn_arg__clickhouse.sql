SELECT 
      c.customer_id AS "c.customer_id", 
      o.order_id AS "o.order_id"
FROM db_fk_edge.customers_fk AS c
LEFT JOIN (SELECT * FROM db_fk_edge.orders_fk WHERE toFloat64(total_amount) > 100) AS o ON o.customer_id = c.customer_id
