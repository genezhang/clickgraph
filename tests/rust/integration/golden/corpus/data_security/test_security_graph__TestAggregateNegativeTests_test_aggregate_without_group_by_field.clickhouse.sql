SELECT 
      u.name AS "u.name", 
      u.email AS "u.email", 
      count(*) AS "COUNT(*)"
FROM data_security.ds_users AS u
GROUP BY u.name, u.email
