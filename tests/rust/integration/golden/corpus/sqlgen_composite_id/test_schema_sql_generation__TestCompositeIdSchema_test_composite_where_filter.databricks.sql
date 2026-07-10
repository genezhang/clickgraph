SELECT 
      c.name AS `c.name`, 
      a.balance AS `a.balance`
FROM db_composite_id.accounts AS a
INNER JOIN db_composite_id.account_ownership AS t0 ON a.bank_id = t0.bank_id AND a.account_number = t0.account_number
INNER JOIN db_composite_id.customers AS c ON t0.customer_id = c.customer_id
WHERE a.balance > 1000
ORDER BY a.balance DESC
