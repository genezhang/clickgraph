WITH with_n_name_cte_0 AS (SELECT 
      c.name AS "name", 
      count(o.order_id) AS "n"
FROM db_fk_edge.orders_fk AS o
INNER JOIN db_fk_edge.customers_fk AS c ON c.customer_id = o.customer_id
GROUP BY c.name
HAVING n > 1
)
SELECT 
      n_name.name AS "name", 
      n_name.n AS "n"
FROM with_n_name_cte_0 AS n_name
