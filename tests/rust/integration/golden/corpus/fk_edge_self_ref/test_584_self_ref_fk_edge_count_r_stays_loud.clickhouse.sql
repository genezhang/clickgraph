WITH with_w1 AS (
SELECT 
      v0.object_id AS "v3__object_id", 
      v0.name AS "p2_v3_name", 
      count(v1.object_id) AS "v4"
FROM test_integration.fs_objects_single AS v0
JOIN test_integration.fs_objects_single AS v1 ON v1.object_id = v0.object_id
JOIN test_integration.fs_objects_single AS v2 ON v1.parent_id = v2.object_id
GROUP BY v0.object_id, v0.name
)
SELECT 
      w1.p2_v3_name AS "c.name", 
      w1.v4 AS "rc"
FROM with_w1 AS w1
