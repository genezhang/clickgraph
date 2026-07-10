WITH RECURSIVE vlp_u1_u2 AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        ['TEST_FOLLOWS::TestUser::TestUser'] as path_relationships,
        [start_node.user_id, end_node.user_id] as path_nodes,
        start_node.name as start_name
    FROM test_integration.users AS start_node
    JOIN test_integration.follows AS rel ON start_node.user_id = rel.follower_id
    JOIN test_integration.users AS end_node ON rel.followed_id = end_node.user_id
    WHERE start_node.user_id = 1
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        arrayConcat(vp.path_relationships, ['TEST_FOLLOWS::TestUser::TestUser']) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.user_id]) as path_nodes,
        vp.start_name as start_name
    FROM vlp_u1_u2 vp
    JOIN test_integration.follows AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_nodes, end_node.user_id)
), 
with_reach_count_u1_cte_0 AS (SELECT 
      anyLast(start_name) AS "p2_u1_name", 
      count(DISTINCT t.end_user_id) AS "reach_count"
FROM vlp_u1_u2 AS t
GROUP BY t.start_id
)
SELECT 
      reach_count_u1.p2_u1_name AS "u1.name", 
      reach_count_u1.reach_count AS "reach_count"
FROM with_reach_count_u1_cte_0 AS reach_count_u1
