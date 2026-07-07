WITH RECURSIVE vlp_a_a AS (
    SELECT 
        start_node.user_id as start_id,
        start_node.user_id as end_id,
        0 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.user_id] as path_nodes
    FROM test_integration.users AS start_node
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.user_id]) as path_nodes
    FROM vlp_a_a vp
    JOIN test_integration.follows AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 3
      AND NOT has(vp.path_nodes, end_node.user_id)
)
SELECT 
      vt0.name AS "a.name", 
      count(*) AS "paths"
FROM test_integration.users AS a
LEFT JOIN vlp_a_a AS vt0 ON a.user_id = vt0.start_id
GROUP BY vt0.name
