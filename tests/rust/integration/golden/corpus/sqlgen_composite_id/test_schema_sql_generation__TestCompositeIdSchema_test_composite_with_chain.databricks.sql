SELECT 
      v0.name AS `c.name`, 
      v2.account_number AS `a.account_number`, 
      v4.account_number AS `b.account_number`, 
      count(*) AS `transfers`
FROM db_composite_id.customers AS v0
JOIN db_composite_id.account_ownership AS v1 ON v1.customer_id = v0.customer_id
JOIN db_composite_id.accounts AS v2 ON v1.bank_id = v2.bank_id AND v1.account_number = v2.account_number
JOIN db_composite_id.transfers AS v3 ON v3.from_bank_id = v2.bank_id AND v3.from_account_number = v2.account_number
JOIN db_composite_id.accounts AS v4 ON v3.to_bank_id = v4.bank_id AND v3.to_account_number = v4.account_number
GROUP BY v0.name, v2.account_number, v4.account_number

-- column c.name: Value [("c.name", "c.name")]
-- column a.account_number: Value [("a.account_number", "a.account_number")]
-- column b.account_number: Value [("b.account_number", "b.account_number")]
-- column transfers: Value [("transfers", "transfers")]