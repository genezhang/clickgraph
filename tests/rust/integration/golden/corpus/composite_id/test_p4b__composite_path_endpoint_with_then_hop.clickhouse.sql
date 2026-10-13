WITH RECURSIVE vlp_v1_path AS (
    SELECT 
        toString(tuple(start_node.bank_id, start_node.account_number)) as start_id,
        toString(tuple(end_node.bank_id, end_node.account_number)) as end_id,
        1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [toString(tuple(start_node.bank_id, start_node.account_number)), toString(tuple(end_node.bank_id, end_node.account_number))] as path_nodes,
        [rel.transfer_id] as path_edges,
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
        toString(tuple(end_node.bank_id, end_node.account_number)) as end_id,
        vp.hop_count + 1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat(vp.path_nodes, [toString(tuple(end_node.bank_id, end_node.account_number))]) as path_nodes,
        arrayConcat(vp.path_edges, [rel.transfer_id]) as path_edges,
        vp.start_bank_id as start_bank_id,
        vp.start_account_number as start_account_number,
        end_node.bank_id as end_bank_id,
        end_node.account_number as end_account_number
    FROM vlp_v1_path vp
    JOIN db_composite_id.transfers AS rel ON vp.end_bank_id = rel.from_bank_id AND vp.end_account_number = rel.from_account_number
    JOIN db_composite_id.accounts AS end_node ON rel.to_bank_id = end_node.bank_id AND rel.to_account_number = end_node.account_number
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, rel.transfer_id)
), 
with_w2 AS (
SELECT 
      v2.bank_id AS "v3__bank_id", 
      v2.account_number AS "v3__account_number"
FROM vlp_v1_path AS v1
JOIN db_composite_id.accounts AS v0 ON v1.start_bank_id = v0.bank_id AND v1.start_account_number = v0.account_number
JOIN db_composite_id.accounts AS v2 ON v1.end_bank_id = v2.bank_id AND v1.end_account_number = v2.account_number
)
SELECT 
      count(*) AS "k"
FROM with_w2 AS w2
JOIN db_composite_id.transfers AS v4 ON v4.from_bank_id = w2.v3__bank_id AND v4.from_account_number = w2.v3__account_number
JOIN db_composite_id.accounts AS v5 ON v4.to_bank_id = v5.bank_id AND v4.to_account_number = v5.account_number
