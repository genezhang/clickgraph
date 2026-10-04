SELECT 
      t0.mgr_id AS "a.pid", 
      t1.mgr_id AS "b.pid", 
      t1.emp_id AS "c.pid"
FROM testdb.reports AS t0
INNER JOIN testdb.reports AS t1 ON t0.emp_id = t1.mgr_id
WHERE NOT (t1.mgr_id = t0.mgr_id AND t1.emp_id = t0.emp_id)
UNION ALL 
SELECT 
      t0.emp_id AS "a.pid", 
      t1.mgr_id AS "b.pid", 
      t1.emp_id AS "c.pid"
FROM testdb.reports AS t0
INNER JOIN testdb.reports AS t1 ON t1.mgr_id = t0.mgr_id
WHERE NOT (t1.mgr_id = t0.mgr_id AND t1.emp_id = t0.emp_id)
