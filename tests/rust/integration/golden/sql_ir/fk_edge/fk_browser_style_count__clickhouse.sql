SELECT count(`n.customer_id`) AS "count(n)" FROM (
SELECT 
      NULL AS "amount",
      toString(n.customer_id) AS "customer_id",
      toString(n.email) AS "email",
      toString(n.name) AS "name",
      NULL AS "order_date",
      NULL AS "order_id",
      toString(n.customer_id) AS "n.customer_id"
FROM db_fk_edge.customers_fk AS n
UNION ALL 
SELECT 
      toString(n.total_amount) AS "amount",
      NULL AS "customer_id",
      NULL AS "email",
      NULL AS "name",
      toString(n.order_date) AS "order_date",
      toString(n.order_id) AS "order_id",
      NULL AS "n.customer_id"
FROM db_fk_edge.orders_fk AS n
) AS __union
