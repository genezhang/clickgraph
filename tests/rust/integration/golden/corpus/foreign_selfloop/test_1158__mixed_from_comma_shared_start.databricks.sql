SELECT 
      t0.mgr_id AS `a.pid`, 
      b.name AS `b.name`, 
      c.name AS `c.name`
FROM testdb.reports AS t1
INNER JOIN testdb.people AS b ON b.pid = t1.emp_id
INNER JOIN testdb.reports AS t0 ON t0.mgr_id = t1.mgr_id
INNER JOIN testdb.people AS c ON c.pid = t0.emp_id
WHERE (t0.mgr_id <> t1.mgr_id OR t0.emp_id <> t1.emp_id)
