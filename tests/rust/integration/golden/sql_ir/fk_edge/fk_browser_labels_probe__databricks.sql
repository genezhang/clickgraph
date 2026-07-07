SELECT DISTINCT 
      array('Customer') AS `labels(n)`
FROM db_fk_edge.customers_fk AS n
UNION ALL 
SELECT DISTINCT 
      array('Order') AS `labels(n)`
FROM db_fk_edge.orders_fk AS n
