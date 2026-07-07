SELECT 
      u.name AS "u.name", 
      u.email AS "u.email"
FROM data_security.ds_users AS u
WHERE u.exposure = 'external'
ORDER BY u.name ASC
