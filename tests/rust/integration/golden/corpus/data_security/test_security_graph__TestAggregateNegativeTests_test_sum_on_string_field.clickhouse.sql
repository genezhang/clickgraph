SELECT 
      coalesce(sum(v0.name), 0) AS "sum_name"
FROM data_security.ds_users AS v0
