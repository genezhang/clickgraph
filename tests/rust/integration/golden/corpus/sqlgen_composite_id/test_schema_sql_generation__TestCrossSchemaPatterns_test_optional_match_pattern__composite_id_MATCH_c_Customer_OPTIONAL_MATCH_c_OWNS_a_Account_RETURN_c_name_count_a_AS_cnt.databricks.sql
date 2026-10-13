WITH optional_o1 AS (
SELECT 
      v1.customer_id AS `v0__customer_id`, 
      v2.bank_id AS `v2__bank_id`, 
      v2.account_number AS `v2__account_number`
FROM db_composite_id.account_ownership AS v1
JOIN db_composite_id.accounts AS v2 ON v1.bank_id = v2.bank_id AND v1.account_number = v2.account_number
)
SELECT 
      v0.name AS `c.name`, 
      count(CASE WHEN (o1.v2__bank_id IS NULL AND o1.v2__account_number IS NULL) THEN NULL ELSE struct(o1.v2__bank_id, o1.v2__account_number) END) AS `cnt`
FROM db_composite_id.customers AS v0
LEFT JOIN optional_o1 AS o1 ON v0.customer_id = o1.v0__customer_id
GROUP BY v0.name
