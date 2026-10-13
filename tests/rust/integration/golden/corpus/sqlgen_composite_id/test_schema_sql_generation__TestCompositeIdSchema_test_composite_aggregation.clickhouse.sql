SELECT 
      v0.name AS "c.name", 
      count(CASE WHEN (v2.bank_id IS NULL AND v2.account_number IS NULL) THEN NULL ELSE tuple(v2.bank_id, v2.account_number) END) AS "accounts", 
      coalesce(sum(v2.balance), 0) AS "total_balance"
FROM db_composite_id.customers AS v0
JOIN db_composite_id.account_ownership AS v1 ON v1.customer_id = v0.customer_id
JOIN db_composite_id.accounts AS v2 ON v1.bank_id = v2.bank_id AND v1.account_number = v2.account_number
GROUP BY v0.name
