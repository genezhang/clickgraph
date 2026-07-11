SELECT 
      c.customer_id AS `c.customer_id`, 
      o.order_id AS `o.order_id`
FROM db_fk_edge.customers_fk AS c
LEFT JOIN (SELECT * FROM db_fk_edge.orders_fk WHERE concat(status, 'X') = 'shippedX') AS o ON o.customer_id = c.customer_id
