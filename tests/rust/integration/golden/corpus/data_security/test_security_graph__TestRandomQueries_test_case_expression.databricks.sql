SELECT 
      u.name AS `u.name`, 
      CASE u.exposure WHEN 'external' THEN 'RISK' ELSE 'OK' END AS `risk_level`
FROM data_security.ds_users AS u
ORDER BY u.name ASC
