SELECT 
      v2.bank_id AS `a2.bank_id`, 
      v2.account_number AS `a2.account_number`, 
      v2.balance AS `a2.balance`
FROM db_composite_id.accounts AS v0
JOIN db_composite_id.transfers AS v1 ON v1.from_bank_id = v0.bank_id AND v1.from_account_number = v0.account_number
JOIN db_composite_id.accounts AS v2 ON v1.to_bank_id = v2.bank_id AND v1.to_account_number = v2.account_number
WHERE (v0.bank_id = 'B001' AND v0.account_number = 'ACC001')
LIMIT 10
-- column a2.bank_id: Value [("a2.bank_id", "a2.bank_id")]
-- column a2.account_number: Value [("a2.account_number", "a2.account_number")]
-- column a2.balance: Value [("a2.balance", "a2.balance")]