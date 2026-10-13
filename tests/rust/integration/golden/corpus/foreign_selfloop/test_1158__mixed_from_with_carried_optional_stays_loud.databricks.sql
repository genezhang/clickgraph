WITH with_w1 AS (
SELECT 
      v0.pid AS `v3__pid`, 
      v0.name AS `p2_v3_name`, 
      v2.pid AS `v4__pid`, 
      v2.name AS `p2_v4_name`
FROM testdb.people AS v0
JOIN testdb.reports AS v1 ON v1.mgr_id = v0.pid
JOIN testdb.people AS v2 ON v1.emp_id = v2.pid
), 
optional_o2 AS (
SELECT 
      v5.mgr_id AS `v4__pid`, 
      v5.emp_id AS `v3__pid`
FROM testdb.reports AS v5
)
SELECT 
      w1.p2_v3_name AS `e.name`, 
      w1.p2_v4_name AS `b.name`
FROM with_w1 AS w1
LEFT JOIN optional_o2 AS o2 ON w1.v4__pid = o2.v4__pid AND w1.v3__pid = o2.v3__pid

-- column e.name: Value [("e.name", "e.name")]
-- column b.name: Value [("b.name", "b.name")]