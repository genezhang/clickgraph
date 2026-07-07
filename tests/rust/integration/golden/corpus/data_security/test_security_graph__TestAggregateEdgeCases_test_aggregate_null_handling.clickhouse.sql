SELECT 
      count(u.user_id) AS "count"
FROM data_security.ds_users AS u
WHERE (u.exposure IS NULL OR u.exposure = 'internal')
