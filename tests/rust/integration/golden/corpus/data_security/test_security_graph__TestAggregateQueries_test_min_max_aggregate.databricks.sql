SELECT 
      min(v0.name) AS `first_alpha`, 
      max(v0.name) AS `last_alpha`
FROM data_security.ds_users AS v0

-- column first_alpha: Value [("first_alpha", "first_alpha")]
-- column last_alpha: Value [("last_alpha", "last_alpha")]