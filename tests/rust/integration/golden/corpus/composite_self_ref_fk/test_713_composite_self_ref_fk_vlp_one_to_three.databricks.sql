WITH RECURSIVE vlp_v1_path AS (
    SELECT 
        to_json(CAST(struct(start_node.region, start_node.object_id) AS STRUCT<_1: STRING, _2: STRING>), map('ignoreNullFields', 'false')) as start_id,
        to_json(CAST(struct(end_node.region, end_node.object_id) AS STRUCT<_1: STRING, _2: STRING>), map('ignoreNullFields', 'false')) as end_id,
        1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        array(to_json(CAST(struct(start_node.region, start_node.object_id) AS STRUCT<_1: STRING, _2: STRING>), map('ignoreNullFields', 'false')), to_json(CAST(struct(end_node.region, end_node.object_id) AS STRUCT<_1: STRING, _2: STRING>), map('ignoreNullFields', 'false'))) as path_nodes,
        array(struct(rel.region, rel.object_id, rel.parent_region, rel.parent_id)) as path_edges,
        start_node.region as start_region,
        start_node.object_id as start_object_id,
        end_node.region as end_region,
        end_node.object_id as end_object_id
    FROM test_integration.fs_objects_composite AS start_node
    JOIN test_integration.fs_objects_composite AS rel ON start_node.region = rel.region AND start_node.object_id = rel.object_id
    JOIN test_integration.fs_objects_composite AS end_node ON rel.parent_region = end_node.region AND rel.parent_id = end_node.object_id
    UNION ALL
    SELECT
        vp.start_id,
        to_json(CAST(struct(end_node.region, end_node.object_id) AS STRUCT<_1: STRING, _2: STRING>), map('ignoreNullFields', 'false')) as end_id,
        vp.hop_count + 1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        concat(vp.path_nodes, array(to_json(CAST(struct(end_node.region, end_node.object_id) AS STRUCT<_1: STRING, _2: STRING>), map('ignoreNullFields', 'false')))) as path_nodes,
        concat(vp.path_edges, array(struct(rel.region, rel.object_id, rel.parent_region, rel.parent_id))) as path_edges,
        vp.start_region as start_region,
        vp.start_object_id as start_object_id,
        end_node.region as end_region,
        end_node.object_id as end_object_id
    FROM vlp_v1_path vp
    JOIN test_integration.fs_objects_composite AS rel ON vp.end_region = rel.region AND vp.end_object_id = rel.object_id
    JOIN test_integration.fs_objects_composite AS end_node ON rel.parent_region = end_node.region AND rel.parent_id = end_node.object_id
    WHERE vp.hop_count < 3
      AND NOT array_contains(vp.path_edges, struct(rel.region, rel.object_id, rel.parent_region, rel.parent_id))
)
SELECT 
      v0.name AS `a.name`, 
      v2.name AS `b.name`
FROM vlp_v1_path AS v1
JOIN test_integration.fs_objects_composite AS v0 ON v1.start_region = v0.region AND v1.start_object_id = v0.object_id
JOIN test_integration.fs_objects_composite AS v2 ON v1.end_region = v2.region AND v1.end_object_id = v2.object_id
