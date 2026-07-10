SELECT 
      count(*) AS `c`
FROM db_fk_edge.orders_fk AS r
UNION ALL 
SELECT 
      count(c2.customer_id) AS `c`
FROM db_fk_edge.customers_fk AS c2
