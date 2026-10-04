SELECT 
      b.pid AS "b.pid", 
      t0.mgr_id AS "a.pid"
FROM testdb.people AS b
LEFT JOIN testdb.reports AS t0 ON t0.emp_id = b.pid
