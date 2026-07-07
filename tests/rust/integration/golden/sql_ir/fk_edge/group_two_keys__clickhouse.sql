SELECT 
      c.name AS "c.name", 
      c.email AS "c.email", 
      count(o.order_id) AS "n"
FROM db_fk_edge.orders_fk AS o
INNER JOIN db_fk_edge.customers_fk AS c ON c.customer_id = o.customer_id
GROUP BY c.name, c.email
