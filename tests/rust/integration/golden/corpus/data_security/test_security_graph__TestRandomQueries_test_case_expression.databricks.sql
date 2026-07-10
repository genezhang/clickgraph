SELECT 
      u.name AS `u.name`, 
      caseWithExpression(u.exposure, 'external', 'RISK', 'OK') AS `risk_level`
FROM data_security.ds_users AS u
ORDER BY u.name ASC
