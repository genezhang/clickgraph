WITH v1_both AS (
SELECT 
      e.emp_id AS `emp_id`, 
      e.mgr_id AS `mgr_id`, 
      e.mgr_id AS `__cg_start_0`, 
      e.emp_id AS `__cg_end_0`
FROM testdb.reports AS e

UNION ALL
SELECT 
      e.emp_id AS `emp_id`, 
      e.mgr_id AS `mgr_id`, 
      e.emp_id AS `__cg_start_0`, 
      e.mgr_id AS `__cg_end_0`
FROM testdb.reports AS e
WHERE e.mgr_id <> e.emp_id
)
SELECT 
      v0.pid AS `a.pid`, 
      v2.pid AS `b.pid`, 
      v4.pid AS `c.pid`
FROM testdb.people AS v0
JOIN v1_both AS v1 ON v1.__cg_start_0 = v0.pid
JOIN testdb.people AS v2 ON v1.__cg_end_0 = v2.pid
JOIN testdb.reports AS v3 ON v3.mgr_id = v2.pid
JOIN testdb.people AS v4 ON v3.emp_id = v4.pid
WHERE (v1.mgr_id <> v3.mgr_id OR v1.emp_id <> v3.emp_id)
