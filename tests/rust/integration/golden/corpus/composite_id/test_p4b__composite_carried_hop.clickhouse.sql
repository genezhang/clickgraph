WITH with_c_cte_0 AS (SELECT 
      c.account_number AS "p1_c_account_number", 
      c.bank_id AS "p1_c_bank_id"
FROM db_composite_id.accounts AS z
INNER JOIN db_composite_id.transfers AS t0 ON t0.from_bank_id = z.bank_id AND t0.from_account_number = z.account_number
INNER JOIN db_composite_id.accounts AS c ON c.bank_id = t0.to_bank_id AND c.account_number = t0.to_account_number
)
SELECT 
      c.p1_c_account_number AS "x", 
      a.account_number AS "y"
FROM with_c_cte_0 AS c
INNER JOIN db_composite_id.transfers AS t1 ON t1.from_bank_id = c.p1_c_bank_id AND t1.from_account_number = c.p1_c_account_number
INNER JOIN db_composite_id.accounts AS a ON a.bank_id = t1.to_bank_id AND a.account_number = t1.to_account_number
