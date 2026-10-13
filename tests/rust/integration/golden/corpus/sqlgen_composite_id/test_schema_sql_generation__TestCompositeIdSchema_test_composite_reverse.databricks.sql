SELECT 
      v2.name AS `c.name`, 
      v0.balance AS `a.balance`
FROM db_composite_id.accounts AS v0
JOIN db_composite_id.account_ownership AS v1 ON v1.bank_id = v0.bank_id AND v1.account_number = v0.account_number
JOIN db_composite_id.customers AS v2 ON v1.customer_id = v2.customer_id

-- column c.name: Value [("c.name", "c.name")]
-- column a.balance: Value [("a.balance", "a.balance")]