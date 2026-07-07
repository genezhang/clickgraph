SELECT 
      a.account_number AS `a.account_number`, 
      a.balance AS `a.balance`
FROM db_composite_id.accounts AS a
ORDER BY a.balance DESC
LIMIT 3 OFFSET 1