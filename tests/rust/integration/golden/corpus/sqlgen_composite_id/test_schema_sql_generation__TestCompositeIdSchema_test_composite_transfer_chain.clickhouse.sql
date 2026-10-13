SELECT 
      v0.account_number AS "a.account_number", 
      v2.account_number AS "b.account_number", 
      v1.amount AS "t.amount"
FROM db_composite_id.accounts AS v0
JOIN db_composite_id.transfers AS v1 ON v1.from_bank_id = v0.bank_id AND v1.from_account_number = v0.account_number
JOIN db_composite_id.accounts AS v2 ON v1.to_bank_id = v2.bank_id AND v1.to_account_number = v2.account_number

-- column a.account_number: Value [("a.account_number", "a.account_number")]
-- column b.account_number: Value [("b.account_number", "b.account_number")]
-- column t.amount: Value [("t.amount", "t.amount")]