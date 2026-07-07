SELECT 
      a.account_number AS "a.account_number"
FROM db_composite_id.accounts AS a
WHERE (a.balance > 1000 AND a.account_type = 'Savings')
