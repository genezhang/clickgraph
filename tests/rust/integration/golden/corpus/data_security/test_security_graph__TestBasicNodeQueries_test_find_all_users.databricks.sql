SELECT 
      v0.name AS `u.name`, 
      v0.email AS `u.email`
FROM data_security.ds_users AS v0
ORDER BY v0.name ASC NULLS LAST

-- column u.name: Value [("u.name", "u.name")]
-- column u.email: Value [("u.email", "u.email")]