WITH with_a_cte_0 AS (SELECT 
      a.account_number AS `p1_a_account_number`, 
      a.bank_id AS `p1_a_bank_id`
FROM db_composite_id.accounts AS a
WHERE a.balance > 5000
)
SELECT 
      a.p1_a_account_number AS `a.account_number`, 
      a2.account_number AS `a2.account_number`
FROM with_a_cte_0 AS a
INNER JOIN db_composite_id.transfers AS t0 ON t0.from_bank_id = a.p1_a_bank_id AND t0.from_account_number = a.p1_a_account_number
INNER JOIN db_composite_id.accounts AS a2 ON a2.bank_id = t0.to_bank_id AND a2.account_number = t0.to_account_number
