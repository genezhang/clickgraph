SELECT 
      v0.name AS `u.name`, 
      v0.email AS `u.email`, 
      count(*) AS `COUNT(*)`
FROM data_security.ds_users AS v0
GROUP BY v0.name, v0.email
