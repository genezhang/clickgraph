SELECT 
      v0.name AS "u.name"
FROM data_security.ds_users AS v0
WHERE v0.nonexistent_prop = 'x'

-- column u.name: Value [("u.name", "u.name")]