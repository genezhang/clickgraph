SELECT 
      count(v0.user_id) AS `count`
FROM data_security.ds_users AS v0
WHERE v0.name = 'NonExistentUser123'

-- column count: Value [("count", "count")]