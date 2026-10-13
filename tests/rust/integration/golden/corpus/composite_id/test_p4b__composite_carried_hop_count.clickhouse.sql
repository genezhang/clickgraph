WITH with_w1 AS (
SELECT 
      v2.bank_id AS "v3__bank_id", 
      v2.account_number AS "v3__account_number"
FROM db_composite_id.accounts AS v0
JOIN db_composite_id.transfers AS v1 ON v1.from_bank_id = v0.bank_id AND v1.from_account_number = v0.account_number
JOIN db_composite_id.accounts AS v2 ON v1.to_bank_id = v2.bank_id AND v1.to_account_number = v2.account_number
)
SELECT 
      count(*) AS "k"
FROM with_w1 AS w1
JOIN db_composite_id.transfers AS v5 ON v5.to_bank_id = w1.v3__bank_id AND v5.to_account_number = w1.v3__account_number
JOIN db_composite_id.accounts AS v4 ON v5.from_bank_id = v4.bank_id AND v5.from_account_number = v4.account_number
