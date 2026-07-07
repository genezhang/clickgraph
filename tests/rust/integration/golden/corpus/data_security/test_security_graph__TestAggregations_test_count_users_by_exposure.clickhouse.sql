SELECT 
      u.exposure AS "u.exposure", 
      count(u.user_id) AS "count"
FROM data_security.ds_users AS u
GROUP BY u.exposure
ORDER BY u.exposure ASC
