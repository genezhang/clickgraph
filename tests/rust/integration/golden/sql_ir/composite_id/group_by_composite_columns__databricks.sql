SELECT 
      a.bank_id AS `a.bank_id`, 
      a.account_number AS `a.account_number`, 
      count(a2.bank_id) AS `n`
FROM db_composite_id.accounts AS a
INNER JOIN db_composite_id.transfers AS t0 ON t0.from_bank_id = a.bank_id AND t0.from_account_number = a.account_number
INNER JOIN db_composite_id.accounts AS a2 ON a2.bank_id = t0.to_bank_id AND a2.account_number = t0.to_account_number
GROUP BY a.bank_id, a.account_number
