SELECT 
      a.bank_id AS "a.bank_id", 
      b.bank_id AS "b.bank_id"
FROM db_composite_id.accounts AS a
INNER JOIN db_composite_id.transfers AS r1 ON a.bank_id = r1.from_bank_id AND a.account_number = r1.from_account_number
INNER JOIN db_composite_id.transfers AS r2 ON r1.to_bank_id = r2.from_bank_id AND r1.to_account_number = r2.from_account_number
INNER JOIN db_composite_id.accounts AS b ON r2.to_bank_id = b.bank_id AND r2.to_account_number = b.account_number
WHERE r1.transfer_id <> r2.transfer_id
