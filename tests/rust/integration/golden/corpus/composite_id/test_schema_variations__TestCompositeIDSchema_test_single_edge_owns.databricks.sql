SELECT 
      v2.bank_id AS `a.bank_id`, 
      v2.account_number AS `a.account_number`, 
      v2.balance AS `a.balance`
FROM db_composite_id.customers AS v0
JOIN db_composite_id.account_ownership AS v1 ON v1.customer_id = v0.customer_id
JOIN db_composite_id.accounts AS v2 ON v1.bank_id = v2.bank_id AND v1.account_number = v2.account_number
WHERE v0.customer_id = 1
LIMIT 10
-- column a.bank_id: Value [("a.bank_id", "a.bank_id")]
-- column a.account_number: Value [("a.account_number", "a.account_number")]
-- column a.balance: Value [("a.balance", "a.balance")]