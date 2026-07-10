WITH RECURSIVE vlp_a_b_inner AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        ['TEST_FOLLOWS'] as path_relationships,
        [start_node.user_id, end_node.user_id] as path_nodes,
        end_node.name as end_name
    FROM test_integration.users AS start_node
    JOIN test_integration.follows AS rel ON start_node.user_id = rel.follower_id
    JOIN test_integration.users AS end_node ON rel.followed_id = end_node.user_id
    WHERE start_node.name = 'Alice'
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        arrayConcat(vp.path_relationships, ['TEST_FOLLOWS']) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.user_id]) as path_nodes,
        end_node.name as end_name
    FROM vlp_a_b_inner vp
    JOIN test_integration.follows AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 5
      AND NOT has(vp.path_nodes, end_node.user_id)
),
vlp_a_b AS (
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY end_id ORDER BY hop_count ASC) as rn
        FROM vlp_a_b_inner
    ) WHERE rn = 1
)
SELECT 
      t.end_name AS "b.name", 
      t.hop_count AS "distance"
FROM vlp_a_b AS t
ORDER BY distance ASC, t.end_name ASC
