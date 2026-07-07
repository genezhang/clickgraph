SELECT 
      r.customer_id AS "r.from_id", 
      r.bank_id AS "r.to_id_1", 
      r.account_number AS "r.to_id_2", 
      r.role AS "r.role", 
      r.since AS "r.since"
FROM db_composite_id.account_ownership AS r
INNER JOIN db_composite_id.accounts AS a ON a.bank_id = r.bank_id AND a.account_number = r.account_number
