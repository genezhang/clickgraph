SELECT 
      o.total_amount AS "o.amount", 
      o.order_date AS "o.order_date", 
      o.order_id AS "o.order_id", 
      c.customer_id AS "c.customer_id", 
      c.email AS "c.email", 
      c.name AS "c.name", 
      tuple('fixed_path', 'o', 'c', 't0') AS "p"
FROM db_fk_edge.orders_fk AS o
INNER JOIN db_fk_edge.customers_fk AS c ON c.customer_id = o.customer_id
