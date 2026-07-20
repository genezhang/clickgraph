SELECT 
      c.name AS `c.name`, 
      p.name AS `p.name`
FROM test_integration.fs_objects_single AS p
INNER JOIN test_integration.fs_objects_single AS c ON c.parent_id = p.object_id
