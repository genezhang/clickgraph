SELECT 
      r.from_bank_id AS `r.from_id_1`, 
      r.from_account_number AS `r.from_id_2`, 
      r.to_bank_id AS `r.to_id_1`, 
      r.to_account_number AS `r.to_id_2`, 
      r.amount AS `r.amount`, 
      r.transfer_date AS `r.transfer_date`, 
      r.transfer_id AS `r.transfer_id`
FROM db_composite_id.transfers AS r
INNER JOIN db_composite_id.accounts AS a2 ON a2.bank_id = r.to_bank_id AND a2.account_number = r.to_account_number
