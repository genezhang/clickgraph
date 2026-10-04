SELECT 
      t0.emp_id AS `b.pid`, 
      t0.mgr_id AS `a.pid`
FROM testdb.people AS b
LEFT JOIN testdb.reports AS t0 ON b.pid = t0.emp_id
