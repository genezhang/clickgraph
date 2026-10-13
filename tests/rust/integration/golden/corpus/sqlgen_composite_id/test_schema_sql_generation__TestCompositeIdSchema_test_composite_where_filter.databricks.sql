SELECT 
      v0.name AS `c.name`, 
      v2.balance AS `a.balance`
FROM db_composite_id.customers AS v0
JOIN db_composite_id.account_ownership AS v1 ON v1.customer_id = v0.customer_id
JOIN db_composite_id.accounts AS v2 ON v1.bank_id = v2.bank_id AND v1.account_number = v2.account_number
WHERE v2.balance > 1000
ORDER BY v2.balance DESC NULLS FIRST
