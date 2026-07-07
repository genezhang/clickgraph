WITH with_c_cte_0 AS (SELECT 
      c.customer_id AS "p1_c_customer_id", 
      c.name AS "p1_c_name"
FROM db_composite_id.customers AS c
WHERE c.customer_id > 2
)
SELECT 
      c.p1_c_name AS "c.name", 
      a.account_number AS "a.account_number"
FROM with_c_cte_0 AS c
INNER JOIN db_composite_id.account_ownership AS t0 ON t0.customer_id = c.p1_c_customer_id
INNER JOIN db_composite_id.accounts AS a ON a.bank_id = t0.bank_id AND a.account_number = t0.account_number
