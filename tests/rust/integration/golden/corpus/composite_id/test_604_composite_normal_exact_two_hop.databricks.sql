WITH RECURSIVE vlp_v1_path_inner AS (
    SELECT 
        to_json(CAST(struct(start_node.bank_id, start_node.account_number) AS STRUCT<_1: STRING, _2: STRING>), map('ignoreNullFields', 'false')) as start_id,
        to_json(CAST(struct(end_node.bank_id, end_node.account_number) AS STRUCT<_1: STRING, _2: STRING>), map('ignoreNullFields', 'false')) as end_id,
        1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        array(to_json(CAST(struct(start_node.bank_id, start_node.account_number) AS STRUCT<_1: STRING, _2: STRING>), map('ignoreNullFields', 'false')), to_json(CAST(struct(end_node.bank_id, end_node.account_number) AS STRUCT<_1: STRING, _2: STRING>), map('ignoreNullFields', 'false'))) as path_nodes,
        array(rel.transfer_id) as path_edges,
        start_node.bank_id as start_bank_id,
        start_node.account_number as start_account_number,
        end_node.bank_id as end_bank_id,
        end_node.account_number as end_account_number
    FROM db_composite_id.accounts AS start_node
    JOIN db_composite_id.transfers AS rel ON start_node.bank_id = rel.from_bank_id AND start_node.account_number = rel.from_account_number
    JOIN db_composite_id.accounts AS end_node ON rel.to_bank_id = end_node.bank_id AND rel.to_account_number = end_node.account_number
    UNION ALL
    SELECT
        vp.start_id,
        to_json(CAST(struct(end_node.bank_id, end_node.account_number) AS STRUCT<_1: STRING, _2: STRING>), map('ignoreNullFields', 'false')) as end_id,
        vp.hop_count + 1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        concat(vp.path_nodes, array(to_json(CAST(struct(end_node.bank_id, end_node.account_number) AS STRUCT<_1: STRING, _2: STRING>), map('ignoreNullFields', 'false')))) as path_nodes,
        concat(vp.path_edges, array(rel.transfer_id)) as path_edges,
        vp.start_bank_id as start_bank_id,
        vp.start_account_number as start_account_number,
        end_node.bank_id as end_bank_id,
        end_node.account_number as end_account_number
    FROM vlp_v1_path_inner vp
    JOIN db_composite_id.transfers AS rel ON vp.end_bank_id = rel.from_bank_id AND vp.end_account_number = rel.from_account_number
    JOIN db_composite_id.accounts AS end_node ON rel.to_bank_id = end_node.bank_id AND rel.to_account_number = end_node.account_number
    WHERE vp.hop_count < 2
      AND NOT array_contains(vp.path_edges, rel.transfer_id)
),
vlp_v1_path AS (
    SELECT * FROM vlp_v1_path_inner WHERE hop_count >= 2
)
SELECT 
      v0.bank_id AS `a.bank_id`, 
      v2.bank_id AS `b.bank_id`
FROM vlp_v1_path AS v1
JOIN db_composite_id.accounts AS v0 ON v1.start_bank_id = v0.bank_id AND v1.start_account_number = v0.account_number
JOIN db_composite_id.accounts AS v2 ON v1.end_bank_id = v2.bank_id AND v1.end_account_number = v2.account_number
