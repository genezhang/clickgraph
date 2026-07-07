WITH RECURSIVE vlp_a_b_inner AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        array('LIKES') as path_relationships,
        array(start_node.user_id, end_node.user_id) as path_nodes
    FROM brahmand.users_bench AS start_node
    JOIN brahmand.interactions AS rel ON start_node.user_id = rel.from_id
    JOIN brahmand.users_bench AS end_node ON rel.to_id = end_node.user_id
    WHERE rel.interaction_type = 'LIKES' AND rel.from_type = 'User' AND rel.to_type = 'User' AND start_node.full_name != end_node.full_name
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        concat(vp.path_relationships, array('LIKES')) as path_relationships,
        concat(vp.path_nodes, array(end_node.user_id)) as path_nodes
    FROM vlp_a_b_inner vp
    JOIN brahmand.interactions AS rel ON vp.end_id = rel.from_id
    JOIN brahmand.users_bench AS end_node ON rel.to_id = end_node.user_id
    WHERE vp.hop_count < 5
      AND NOT array_contains(vp.path_nodes, end_node.user_id)
      AND rel.interaction_type = 'LIKES' AND rel.from_type = 'User' AND rel.to_type = 'User'
),
vlp_a_b AS (
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY end_id ORDER BY hop_count ASC) as rn
        FROM vlp_a_b_inner
    ) WHERE rn = 1
)
SELECT 
      t.hop_count AS `length(p)`
FROM vlp_a_b AS t
LIMIT 5