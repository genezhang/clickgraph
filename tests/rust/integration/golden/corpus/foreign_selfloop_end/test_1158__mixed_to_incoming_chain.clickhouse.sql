SELECT 
      t0.mgr_id AS "a.pid", 
      t0.emp_id AS "b.pid", 
      t1.emp_id AS "c.pid"
FROM testdb.reports AS t1
INNER JOIN testdb.reports AS t0 ON t1.mgr_id = t0.emp_id
WHERE (t0.mgr_id <> t1.mgr_id OR t0.emp_id <> t1.emp_id)
