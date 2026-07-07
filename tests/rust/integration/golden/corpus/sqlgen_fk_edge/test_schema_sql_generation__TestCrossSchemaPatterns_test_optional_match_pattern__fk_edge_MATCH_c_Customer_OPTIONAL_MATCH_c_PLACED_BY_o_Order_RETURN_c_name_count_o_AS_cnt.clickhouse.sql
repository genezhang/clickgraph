SELECT 
      c.name AS "c.name", 
      count(o.order_id) AS "cnt"
FROM db_fk_edge.customers_fk AS c
LEFT JOIN db_fk_edge.orders_fk AS o ON o.customer_id = c.customer_id
GROUP BY c.name
