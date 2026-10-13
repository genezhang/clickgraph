SELECT 
      v0.name AS `u.name`, 
      CASE v0.exposure WHEN 'external' THEN 'RISK' ELSE 'OK' END AS `risk_level`
FROM data_security.ds_users AS v0
ORDER BY v0.name ASC NULLS LAST
