SELECT 
      a.balance AS `a.balance`
FROM db_composite_id.accounts AS a
WHERE (a.bank_id = 'CHASE' AND a.account_number = 'CHK-001')
