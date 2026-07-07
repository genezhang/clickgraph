SELECT 
      c.name AS "c.name", 
      count(o.order_id) AS "cnt"
FROM db_fk_edge.orders_fk AS o
INNER JOIN db_fk_edge.customers_fk AS c ON c.customer_id = o.customer_id
GROUP BY c.name
ORDER BY cnt DESC
LIMIT 5