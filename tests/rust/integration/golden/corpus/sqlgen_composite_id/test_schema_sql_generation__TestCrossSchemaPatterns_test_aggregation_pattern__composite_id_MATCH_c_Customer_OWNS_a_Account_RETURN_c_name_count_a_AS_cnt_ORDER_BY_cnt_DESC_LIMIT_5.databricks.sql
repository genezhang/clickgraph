SELECT 
      v0.name AS `c.name`, 
      count(CASE WHEN (v2.bank_id IS NULL AND v2.account_number IS NULL) THEN NULL ELSE struct(v2.bank_id, v2.account_number) END) AS `cnt`
FROM db_composite_id.customers AS v0
JOIN db_composite_id.account_ownership AS v1 ON v1.customer_id = v0.customer_id
JOIN db_composite_id.accounts AS v2 ON v1.bank_id = v2.bank_id AND v1.account_number = v2.account_number
GROUP BY v0.name
ORDER BY count(CASE WHEN (v2.bank_id IS NULL AND v2.account_number IS NULL) THEN NULL ELSE struct(v2.bank_id, v2.account_number) END) DESC NULLS FIRST
LIMIT 5
-- column c.name: Value [("c.name", "c.name")]
-- column cnt: Value [("cnt", "cnt")]