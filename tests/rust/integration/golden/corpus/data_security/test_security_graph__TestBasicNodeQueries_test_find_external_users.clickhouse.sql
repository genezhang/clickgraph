SELECT 
      v0.name AS "u.name", 
      v0.email AS "u.email"
FROM data_security.ds_users AS v0
WHERE v0.exposure = 'external'
ORDER BY v0.name ASC
