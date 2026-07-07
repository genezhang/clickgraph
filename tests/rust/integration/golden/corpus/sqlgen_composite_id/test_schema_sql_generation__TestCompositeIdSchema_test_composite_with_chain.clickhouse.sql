SELECT 
      c.name AS "c.name", 
      a.account_number AS "a.account_number", 
      b.account_number AS "b.account_number", 
      count(*) AS "transfers"
FROM db_composite_id.customers AS c
INNER JOIN db_composite_id.account_ownership AS t0 ON t0.customer_id = c.customer_id
INNER JOIN db_composite_id.accounts AS a ON a.bank_id = t0.bank_id AND a.account_number = t0.account_number
INNER JOIN db_composite_id.transfers AS t1 ON t1.from_bank_id = a.bank_id AND t1.from_account_number = a.account_number
INNER JOIN db_composite_id.accounts AS b ON b.bank_id = t1.to_bank_id AND b.account_number = t1.to_account_number
GROUP BY c.name, a.account_number, b.account_number
