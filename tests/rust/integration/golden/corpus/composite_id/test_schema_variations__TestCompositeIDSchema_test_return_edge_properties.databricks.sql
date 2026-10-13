SELECT 
      v1.amount AS `r.amount`, 
      v1.transfer_date AS `r.transfer_date`, 
      v2.bank_id AS `a2.bank_id`
FROM db_composite_id.accounts AS v0
JOIN db_composite_id.transfers AS v1 ON v1.from_bank_id = v0.bank_id AND v1.from_account_number = v0.account_number
JOIN db_composite_id.accounts AS v2 ON v1.to_bank_id = v2.bank_id AND v1.to_account_number = v2.account_number
WHERE v0.bank_id = 'B001'
LIMIT 5
-- column r.amount: Value [("r.amount", "r.amount")]
-- column r.transfer_date: Value [("r.transfer_date", "r.transfer_date")]
-- column a2.bank_id: Value [("a2.bank_id", "a2.bank_id")]