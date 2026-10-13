WITH v1_both AS (
SELECT 
      e.amount AS `amount`, 
      e.from_account_number AS `from_account_number`, 
      e.from_bank_id AS `from_bank_id`, 
      e.to_account_number AS `to_account_number`, 
      e.to_bank_id AS `to_bank_id`, 
      e.transfer_date AS `transfer_date`, 
      e.transfer_id AS `transfer_id`, 
      e.from_bank_id AS `__cg_start_0`, 
      e.from_account_number AS `__cg_start_1`, 
      e.to_bank_id AS `__cg_end_0`, 
      e.to_account_number AS `__cg_end_1`
FROM db_composite_id.transfers AS e

UNION ALL
SELECT 
      e.amount AS `amount`, 
      e.from_account_number AS `from_account_number`, 
      e.from_bank_id AS `from_bank_id`, 
      e.to_account_number AS `to_account_number`, 
      e.to_bank_id AS `to_bank_id`, 
      e.transfer_date AS `transfer_date`, 
      e.transfer_id AS `transfer_id`, 
      e.to_bank_id AS `__cg_start_0`, 
      e.to_account_number AS `__cg_start_1`, 
      e.from_bank_id AS `__cg_end_0`, 
      e.from_account_number AS `__cg_end_1`
FROM db_composite_id.transfers AS e
WHERE (e.from_bank_id <> e.to_bank_id OR e.from_account_number <> e.to_account_number)
), 
with_w2 AS (
SELECT 
      v0.bank_id AS `v3__bank_id`, 
      v0.account_number AS `v3__account_number`, 
      v0.account_number AS `p2_v3_account_number`, 
      v2.bank_id AS `v4__bank_id`, 
      v2.account_number AS `v4__account_number`, 
      v2.account_number AS `p2_v4_account_number`
FROM db_composite_id.accounts AS v0
JOIN v1_both AS v1 ON v1.__cg_start_0 = v0.bank_id AND v1.__cg_start_1 = v0.account_number
JOIN db_composite_id.accounts AS v2 ON v1.__cg_end_0 = v2.bank_id AND v1.__cg_end_1 = v2.account_number
)
SELECT 
      w2.p2_v3_account_number AS `a.account_number`, 
      w2.p2_v4_account_number AS `b.account_number`
FROM with_w2 AS w2

-- column a.account_number: Value [("a.account_number", "a.account_number")]
-- column b.account_number: Value [("b.account_number", "b.account_number")]