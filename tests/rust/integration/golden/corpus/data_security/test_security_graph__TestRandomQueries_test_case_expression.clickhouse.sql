SELECT 
      v0.name AS "u.name", 
      caseWithExpression(v0.exposure, 'external', 'RISK', 'OK') AS "risk_level"
FROM data_security.ds_users AS v0
ORDER BY v0.name ASC
