SELECT 
      u.name AS `u.name`
FROM data_security.ds_users AS u
ORDER BY u.name ASC NULLS LAST
LIMIT 3 OFFSET 2