SELECT 
      count(sum(u.user_id)) AS "nested"
FROM data_security.ds_users AS u
