SELECT 
      minOrNull(v0.name) AS "first_alpha", 
      maxOrNull(v0.name) AS "last_alpha"
FROM data_security.ds_users AS v0
