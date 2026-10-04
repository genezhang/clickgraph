SELECT 
      a.name AS "a.name", 
      b.name AS "b.name", 
      c.name AS "c.name"
FROM testdb.reports AS t0
LEFT JOIN (SELECT pid, any(name) as name FROM testdb.people GROUP BY pid) AS a ON a.pid = t0.emp_id
INNER JOIN testdb.people AS b ON b.pid = t0.mgr_id
INNER JOIN testdb.reports AS t1 ON t1.mgr_id = b.pid
LEFT JOIN (SELECT pid, any(name) as name FROM testdb.people GROUP BY pid) AS c ON c.pid = t1.emp_id
LEFT JOIN testdb.reports AS t2 ON t2.mgr_id = t0.emp_id AND t2.emp_id = t1.emp_id
WHERE (t1.mgr_id <> t0.mgr_id OR t1.emp_id <> t0.emp_id)
