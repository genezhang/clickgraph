SELECT 
      a2.bank_id AS "a2.bank_id", 
      a2.account_number AS "a2.account_number", 
      a2.balance AS "a2.balance"
FROM db_composite_id.accounts AS a1
INNER JOIN db_composite_id.transfers AS t0 ON t0.from_bank_id = a1.bank_id AND t0.from_account_number = a1.account_number
INNER JOIN db_composite_id.accounts AS a2 ON a2.bank_id = t0.to_bank_id AND a2.account_number = t0.to_account_number
WHERE (a1.bank_id = 'B001' AND a1.account_number = 'ACC001')
LIMIT 10