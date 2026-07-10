WITH RECURSIVE vlp_a_b AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.user_id, end_node.user_id] as path_nodes,
        start_node.age as start_age,
        start_node.name as start_name,
        end_node.age as end_age,
        end_node.name as end_name
    FROM test_integration.users AS start_node
    JOIN test_integration.follows AS rel ON start_node.user_id = rel.follower_id
    JOIN test_integration.users AS end_node ON rel.followed_id = end_node.user_id
    WHERE start_node.name = 'Bob'
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.user_id]) as path_nodes,
        vp.start_age as start_age,
        vp.start_name as start_name,
        end_node.age as end_age,
        end_node.name as end_name
    FROM vlp_a_b vp
    JOIN test_integration.follows AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_nodes, end_node.user_id)
)
SELECT 
      t.start_name AS "a.name", 
      t.start_age AS "a.age", 
      t.end_name AS "b.name", 
      t.end_age AS "b.age"
FROM vlp_a_b AS t
ORDER BY t.end_name ASC
LIMIT 3