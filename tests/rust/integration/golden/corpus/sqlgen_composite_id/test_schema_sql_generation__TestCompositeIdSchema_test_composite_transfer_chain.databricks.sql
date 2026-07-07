SELECT 
      a.account_number AS `a.account_number`, 
      b.account_number AS `b.account_number`, 
      t.amount AS `t.amount`
FROM db_composite_id.accounts AS a
INNER JOIN db_composite_id.transfers AS t ON t.from_bank_id = a.bank_id AND t.from_account_number = a.account_number
INNER JOIN db_composite_id.accounts AS b ON b.bank_id = t.to_bank_id AND b.account_number = t.to_account_number
