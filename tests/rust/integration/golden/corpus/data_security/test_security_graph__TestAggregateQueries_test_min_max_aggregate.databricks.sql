SELECT 
      min(v0.name) AS `first_alpha`, 
      max(v0.name) AS `last_alpha`
FROM data_security.ds_users AS v0
