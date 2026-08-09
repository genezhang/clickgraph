SELECT 
      minOrNull(u.name) AS "first_alpha", 
      maxOrNull(u.name) AS "last_alpha"
FROM data_security.ds_users AS u
