WITH optional_o1 AS (
SELECT 
      v3.mgr_id AS "v2__pid", 
      v4.pid AS "v4__pid", 
      v4.pid AS "p2_v4_pid"
FROM testdb.reports AS v3
JOIN testdb.people AS v4 ON v3.emp_id = v4.pid
)
SELECT 
      v0.pid AS "a.pid", 
      v2.pid AS "b.pid", 
      o1.p2_v4_pid AS "c.pid"
FROM testdb.people AS v0
JOIN testdb.reports AS v1 ON v1.mgr_id = v0.pid
JOIN testdb.people AS v2 ON v1.emp_id = v2.pid
LEFT JOIN optional_o1 AS o1 ON v2.pid = o1.v2__pid

-- column a.pid: Value [("a.pid", "a.pid")]
-- column b.pid: Value [("b.pid", "b.pid")]
-- column c.pid: Value [("c.pid", "c.pid")]