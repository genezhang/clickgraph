SELECT 
      t0.emp_id AS "a.pid", 
      t1.emp_id AS "b.pid", 
      t2.emp_id AS "c.pid"
FROM testdb.reports AS t1
INNER JOIN testdb.reports AS t2 ON t2.mgr_id = t1.emp_id
INNER JOIN testdb.reports AS t0 ON t0.mgr_id = t2.emp_id AND t1.mgr_id = t0.emp_id
WHERE (((t0.mgr_id <> t2.mgr_id OR t0.emp_id <> t2.emp_id) AND (t0.mgr_id <> t1.mgr_id OR t0.emp_id <> t1.emp_id)) AND (t2.mgr_id <> t1.mgr_id OR t2.emp_id <> t1.emp_id))
