SELECT 
      t0.mgr_id AS "a.pid", 
      t1.mgr_id AS "b.pid", 
      t2.mgr_id AS "c.pid"
FROM testdb.reports AS t0
INNER JOIN testdb.reports AS t1 ON t0.emp_id = t1.mgr_id
INNER JOIN testdb.reports AS t2 ON t2.emp_id = t0.mgr_id AND t1.emp_id = t2.mgr_id
WHERE (((t2.mgr_id <> t1.mgr_id OR t2.emp_id <> t1.emp_id) AND (t2.mgr_id <> t0.mgr_id OR t2.emp_id <> t0.emp_id)) AND (t1.mgr_id <> t0.mgr_id OR t1.emp_id <> t0.emp_id))
