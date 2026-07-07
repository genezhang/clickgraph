SELECT 
      r.amount AS `r.amount`, 
      r.transfer_date AS `r.transfer_date`, 
      a2.bank_id AS `a2.bank_id`
FROM db_composite_id.accounts AS a1
INNER JOIN db_composite_id.transfers AS r ON r.from_bank_id = a1.bank_id AND r.from_account_number = a1.account_number
INNER JOIN db_composite_id.accounts AS a2 ON a2.bank_id = r.to_bank_id AND a2.account_number = r.to_account_number
WHERE a1.bank_id = 'B001'
LIMIT 5