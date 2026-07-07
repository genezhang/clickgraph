SELECT 
      a.account_number AS "a.account_number", 
      a.account_type AS "a.account_type", 
      a.balance AS "a.balance", 
      a.bank_id AS "a.bank_id", 
      a.holder_name AS "a.holder_name", 
      a.opened_date AS "a.opened_date"
FROM db_composite_id.accounts AS a
