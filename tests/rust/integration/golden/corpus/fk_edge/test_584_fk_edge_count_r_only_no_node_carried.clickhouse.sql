WITH with_rc_cte_0 AS (SELECT 
      count(r.order_id) AS "rc"
FROM test_integration.orders_fk AS r
)
SELECT 
      rc.rc AS "rc"
FROM with_rc_cte_0 AS rc
