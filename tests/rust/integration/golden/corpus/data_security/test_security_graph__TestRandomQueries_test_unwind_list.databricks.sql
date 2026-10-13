WITH with_w1 AS (
SELECT 
      __cg_element AS `v0`, 
      __cg_position AS `__o0`
FROM (SELECT 1) AS __cg_one
LATERAL VIEW posexplode(array('Alice', 'Bob', 'Charlie')) AS __cg_position, __cg_element
)
SELECT 
      v1.name AS `u.name`, 
      v1.email AS `u.email`
FROM with_w1 AS w1
CROSS JOIN data_security.ds_users AS v1
WHERE v1.name = w1.v0

-- column u.name: Value [("u.name", "u.name")]
-- column u.email: Value [("u.email", "u.email")]