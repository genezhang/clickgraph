SELECT 
      v0.name AS "u.name", 
      CASE WHEN v0.exposure = 'external' THEN 'RISK' ELSE 'OK' END AS "risk_level"
FROM data_security.ds_users AS v0
ORDER BY v0.name ASC

-- column u.name: Value [("u.name", "u.name")]
-- column risk_level: Value [("risk_level", "risk_level")]