SELECT 
      v0.exposure AS `u.exposure`, 
      count(v0.user_id) AS `count`
FROM data_security.ds_users AS v0
GROUP BY v0.exposure
ORDER BY v0.exposure ASC NULLS LAST

-- column u.exposure: Value [("u.exposure", "u.exposure")]
-- column count: Value [("count", "count")]