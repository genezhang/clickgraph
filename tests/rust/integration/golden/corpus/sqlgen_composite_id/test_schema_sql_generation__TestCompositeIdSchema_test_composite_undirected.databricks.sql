SELECT 
      a.account_number AS `a.account_number`, 
      b.account_number AS `b.account_number`
FROM db_composite_id.accounts AS a
INNER JOIN db_composite_id.transfers AS t0 ON t0.from_bank_id = a.bank_id AND t0.from_account_number = a.account_number
INNER JOIN db_composite_id.accounts AS b ON b.bank_id = t0.to_bank_id AND b.account_number = t0.to_account_number
UNION ALL 
SELECT 
      a.account_number AS `a.account_number`, 
      b.account_number AS `b.account_number`
FROM db_composite_id.accounts AS b
INNER JOIN db_composite_id.transfers AS t0 ON t0.from_bank_id = b.bank_id AND t0.from_account_number = b.account_number
INNER JOIN db_composite_id.accounts AS a ON a.bank_id = t0.to_bank_id AND a.account_number = t0.to_account_number
