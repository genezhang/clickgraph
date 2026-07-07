SELECT 
      o.total_amount AS "o.amount", 
      o.order_date AS "o.order_date", 
      o.order_id AS "o.order_id"
FROM db_fk_edge.orders_fk AS o
