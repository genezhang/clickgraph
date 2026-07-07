SELECT 
      a.bank_id AS `a.bank_id`, 
      a.account_number AS `a.account_number`, 
      a.balance AS `a.balance`
FROM db_composite_id.customers AS c
INNER JOIN db_composite_id.account_ownership AS t0 ON t0.customer_id = c.customer_id
INNER JOIN db_composite_id.accounts AS a ON a.bank_id = t0.bank_id AND a.account_number = t0.account_number
WHERE c.customer_id = 1
LIMIT 10