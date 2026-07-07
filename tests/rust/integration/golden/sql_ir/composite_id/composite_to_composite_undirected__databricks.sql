SELECT 
      a1.account_number AS `a1.account_number`, 
      a2.account_number AS `a2.account_number`
FROM db_composite_id.accounts AS a1
INNER JOIN db_composite_id.transfers AS t0 ON t0.from_bank_id = a1.bank_id AND t0.from_account_number = a1.account_number
INNER JOIN db_composite_id.accounts AS a2 ON a2.bank_id = t0.to_bank_id AND a2.account_number = t0.to_account_number
UNION ALL 
SELECT 
      a1.account_number AS `a1.account_number`, 
      a2.account_number AS `a2.account_number`
FROM db_composite_id.accounts AS a2
INNER JOIN db_composite_id.transfers AS t0 ON t0.from_bank_id = a2.bank_id AND t0.from_account_number = a2.account_number
INNER JOIN db_composite_id.accounts AS a1 ON a1.bank_id = t0.to_bank_id AND a1.account_number = t0.to_account_number
