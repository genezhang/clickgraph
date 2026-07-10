SELECT 
      u.department AS `department`, 
      u.email AS `email`, 
      u.exposure AS `exposure`, 
      u.name AS `name`, 
      u.user_id AS `user_id`
FROM data_security.ds_users AS u
WHERE ((u.exposure = 'external' OR u.name = 'Alice') AND u.email)
