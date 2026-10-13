WITH RECURSIVE v1_both AS (
SELECT 
      e.followed_id AS `followed_id`, 
      e.follower_id AS `follower_id`, 
      e.since AS `since`, 
      e.follower_id AS `__cg_start_0`, 
      e.followed_id AS `__cg_end_0`
FROM test_integration.follows AS e

UNION ALL
SELECT 
      e.followed_id AS `followed_id`, 
      e.follower_id AS `follower_id`, 
      e.since AS `since`, 
      e.followed_id AS `__cg_start_0`, 
      e.follower_id AS `__cg_end_0`
FROM test_integration.follows AS e
WHERE e.follower_id <> e.followed_id
), 
vlp_v1_path_inner AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        array(start_node.user_id, end_node.user_id) as path_nodes,
        array(struct(rel.follower_id, rel.followed_id)) as path_edges
    FROM test_integration.users AS start_node
    JOIN v1_both AS rel ON start_node.user_id = rel.__cg_start_0
    JOIN test_integration.users AS end_node ON rel.__cg_end_0 = end_node.user_id
    WHERE (start_node.name = 'Alice')
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        concat(vp.path_nodes, array(end_node.user_id)) as path_nodes,
        concat(vp.path_edges, array(struct(rel.follower_id, rel.followed_id))) as path_edges
    FROM vlp_v1_path_inner vp
    JOIN v1_both AS rel ON vp.end_id = rel.__cg_start_0
    JOIN test_integration.users AS end_node ON rel.__cg_end_0 = end_node.user_id
    WHERE vp.hop_count < 2
      AND NOT array_contains(vp.path_edges, struct(rel.follower_id, rel.followed_id))
),
vlp_v1_path AS (
    SELECT * FROM vlp_v1_path_inner WHERE hop_count >= 2
)
SELECT DISTINCT 
      v2.name AS `b.name`
FROM vlp_v1_path AS v1
JOIN test_integration.users AS v0 ON v1.start_id = v0.user_id
JOIN test_integration.users AS v2 ON v1.end_id = v2.user_id
WHERE v0.name = 'Alice'
ORDER BY `b.name` ASC NULLS LAST

-- column b.name: Value [("b.name", "b.name")]