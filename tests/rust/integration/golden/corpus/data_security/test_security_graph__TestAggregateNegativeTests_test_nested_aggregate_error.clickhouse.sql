SELECT 
      count(coalesce(sum(v0.user_id), 0)) AS "nested"
FROM data_security.ds_users AS v0

-- column nested: Value [("nested", "nested")]