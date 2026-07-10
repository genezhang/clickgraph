SELECT 
      u.name AS `u.name`
FROM data_security.ds_users AS u
WHERE u.nonexistent_prop = 'x'
