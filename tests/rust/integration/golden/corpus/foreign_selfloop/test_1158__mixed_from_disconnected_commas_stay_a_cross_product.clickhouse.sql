SELECT 
      v0.pid AS "a.pid", 
      v5.pid AS "d.pid"
FROM testdb.people AS v0
JOIN testdb.reports AS v1 ON v1.mgr_id = v0.pid
JOIN testdb.people AS v2 ON v1.emp_id = v2.pid
CROSS JOIN testdb.people AS v3
JOIN testdb.reports AS v4 ON v4.mgr_id = v3.pid
JOIN testdb.people AS v5 ON v4.emp_id = v5.pid
WHERE (v1.mgr_id <> v4.mgr_id OR v1.emp_id <> v4.emp_id)

-- column a.pid: Value [("a.pid", "a.pid")]
-- column d.pid: Value [("d.pid", "d.pid")]