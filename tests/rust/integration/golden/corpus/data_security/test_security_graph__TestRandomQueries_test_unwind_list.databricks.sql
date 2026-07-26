SELECT 
      u.name AS `u.name`, 
      u.email AS `u.email`
FROM data_security.ds_users AS u
LATERAL VIEW explode(array('Alice', 'Bob', 'Charlie')) AS name
WHERE u.name = name
