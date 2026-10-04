SELECT 
      t0.mgr_id AS "a.pid", 
      t1.emp_id AS "d.pid"
FROM testdb.reports AS t0
JOIN testdb.reports AS t1 ON 1 = 1
WHERE (t0.mgr_id <> t1.mgr_id OR t0.emp_id <> t1.emp_id)
