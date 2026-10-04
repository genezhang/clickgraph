SELECT 
      t0.mgr_id AS `a.pid`, 
      t1.mgr_id AS `b.pid`, 
      t1.emp_id AS `c.pid`
FROM testdb.reports AS t2
INNER JOIN testdb.reports AS t1 ON t2.emp_id = t1.mgr_id
INNER JOIN testdb.reports AS t0 ON t0.emp_id = t1.emp_id AND t0.mgr_id = t2.mgr_id
WHERE (((t0.mgr_id <> t1.mgr_id OR t0.emp_id <> t1.emp_id) AND (t0.mgr_id <> t2.mgr_id OR t0.emp_id <> t2.emp_id)) AND (t1.mgr_id <> t2.mgr_id OR t1.emp_id <> t2.emp_id))
