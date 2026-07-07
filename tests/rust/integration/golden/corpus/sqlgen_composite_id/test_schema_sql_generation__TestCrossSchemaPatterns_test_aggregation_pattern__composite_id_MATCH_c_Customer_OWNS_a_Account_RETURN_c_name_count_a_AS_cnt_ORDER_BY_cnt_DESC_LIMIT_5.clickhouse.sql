SELECT 
      c.name AS "c.name", 
      count(a.bank_id) AS "cnt"
FROM db_composite_id.customers AS c
INNER JOIN db_composite_id.account_ownership AS t0 ON t0.customer_id = c.customer_id
INNER JOIN db_composite_id.accounts AS a ON a.bank_id = t0.bank_id AND a.account_number = t0.account_number
GROUP BY c.name
ORDER BY cnt DESC
LIMIT 5