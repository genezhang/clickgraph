SELECT 
      v0.name AS `c.name`, 
      v2.name AS `p.name`
FROM test_integration.fs_objects_single AS v0
JOIN test_integration.fs_objects_single AS v1 ON v1.object_id = v0.object_id
JOIN test_integration.fs_objects_single AS v2 ON v1.parent_id = v2.object_id

-- column c.name: Value [("c.name", "c.name")]
-- column p.name: Value [("p.name", "p.name")]