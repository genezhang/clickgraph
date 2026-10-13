WITH optional_o1 AS (
SELECT 
      v2.emp_id AS `v0__pid`, 
      v1.pid AS `v1__pid`, 
      v1.pid AS `p2_v1_pid`
FROM testdb.people AS v1
JOIN testdb.reports AS v2 ON v2.mgr_id = v1.pid
)
SELECT 
      v0.pid AS `b.pid`, 
      o1.p2_v1_pid AS `a.pid`
FROM testdb.people AS v0
LEFT JOIN optional_o1 AS o1 ON v0.pid = o1.v0__pid

-- column b.pid: Value [("b.pid", "b.pid")]
-- column a.pid: Value [("a.pid", "a.pid")]