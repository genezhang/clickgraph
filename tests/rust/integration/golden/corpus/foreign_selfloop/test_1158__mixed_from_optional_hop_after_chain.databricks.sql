SELECT 
      t0.mgr_id AS `a.pid`, 
      t1.mgr_id AS `b.pid`, 
      t1.emp_id AS `c.pid`
FROM testdb.reports AS t0
LEFT JOIN testdb.reports AS t1 ON t0.emp_id = t1.mgr_id
