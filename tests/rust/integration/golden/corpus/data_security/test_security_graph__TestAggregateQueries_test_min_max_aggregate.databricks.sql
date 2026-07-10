SELECT 
      min(u.name) AS `first_alpha`, 
      max(u.name) AS `last_alpha`
FROM data_security.ds_users AS u
