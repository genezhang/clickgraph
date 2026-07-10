SELECT 
      a.order_id AS "a.order_id", 
      a.customer_id AS "c.customer_id", 
      b.order_id AS "b.order_id"
FROM db_fk_edge.orders_fk AS a
INNER JOIN db_fk_edge.orders_fk AS b ON b.customer_id = a.customer_id
WHERE NOT b.order_id = a.order_id
