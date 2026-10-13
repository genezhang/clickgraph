SELECT 
      v0.name AS "a.name", 
      v2.name AS "b.name", 
      v4.name AS "c.name"
FROM testdb.people AS v0
JOIN testdb.reports AS v1 ON v1.emp_id = v0.pid
JOIN testdb.people AS v2 ON v1.mgr_id = v2.pid
JOIN testdb.reports AS v3 ON v3.mgr_id = v2.pid
JOIN testdb.people AS v4 ON v3.emp_id = v4.pid
WHERE (v1.mgr_id <> v3.mgr_id OR v1.emp_id <> v3.emp_id)

-- column a.name: Value [("a.name", "a.name")]
-- column b.name: Value [("b.name", "b.name")]
-- column c.name: Value [("c.name", "c.name")]