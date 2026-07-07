SELECT count(`n.customer_id`) AS `count(n)` FROM (
SELECT 
      NULL AS `amount`,
      string(n.customer_id) AS `customer_id`,
      string(n.email) AS `email`,
      string(n.name) AS `name`,
      NULL AS `order_date`,
      NULL AS `order_id`,
      string(n.customer_id) AS `n.customer_id`
FROM db_fk_edge.customers_fk AS n
UNION ALL 
SELECT 
      string(n.total_amount) AS `amount`,
      NULL AS `customer_id`,
      NULL AS `email`,
      NULL AS `name`,
      string(n.order_date) AS `order_date`,
      string(n.order_id) AS `order_id`,
      NULL AS `n.customer_id`
FROM db_fk_edge.orders_fk AS n
) AS __union
