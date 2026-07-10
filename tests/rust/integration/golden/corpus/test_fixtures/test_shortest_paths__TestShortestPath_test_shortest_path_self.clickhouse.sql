WITH RECURSIVE vlp_a_a_inner AS (
    SELECT 
        start_node.user_id as start_id,
        start_node.user_id as end_id,
        0 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.user_id] as path_nodes,
        start_node.name as end_name
    FROM test_integration.users AS start_node
    WHERE start_node.name = 'Alice'
),
vlp_a_a AS (
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY end_id ORDER BY hop_count ASC) as rn
        FROM vlp_a_a_inner
    ) WHERE rn = 1
)
SELECT 
      t.end_name AS "a.name"
FROM vlp_a_a AS t
