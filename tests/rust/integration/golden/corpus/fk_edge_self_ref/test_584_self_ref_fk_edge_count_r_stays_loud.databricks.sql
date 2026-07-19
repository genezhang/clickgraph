WITH with_c_rc_cte_0 AS (SELECT 
      any_value(c.name) AS `p1_c_name`, 
      count(r.parent_id) AS `rc`
FROM test_integration.fs_objects_single AS p
INNER JOIN test_integration.fs_objects_single AS c ON c.object_id = p.parent_id
GROUP BY c.object_id
)
SELECT 
      c_rc.p1_c_name AS `c.name`, 
      c_rc.rc AS `rc`
FROM with_c_rc_cte_0 AS c_rc
