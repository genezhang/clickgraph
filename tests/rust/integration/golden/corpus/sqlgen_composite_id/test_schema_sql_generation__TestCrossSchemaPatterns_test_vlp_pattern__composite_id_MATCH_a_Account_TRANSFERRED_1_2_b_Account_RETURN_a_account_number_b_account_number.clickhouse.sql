WITH RECURSIVE vlp_a_b AS (
    SELECT 
        concat(toString(start_node.bank_id), '|', toString(start_node.account_number)) as start_id,
        concat(toString(end_node.bank_id), '|', toString(end_node.account_number)) as end_id,
        1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [concat(toString(start_node.bank_id), '|', toString(start_node.account_number)), concat(toString(end_node.bank_id), '|', toString(end_node.account_number))] as path_nodes,
        [rel.transfer_id] as path_edges,
        start_node.account_number as start_account_number,
        end_node.account_number as end_account_number
    FROM db_composite_id.accounts AS start_node
    JOIN db_composite_id.transfers AS rel ON start_node.bank_id = rel.from_bank_id AND start_node.account_number = rel.from_account_number
    JOIN db_composite_id.accounts AS end_node ON rel.to_bank_id = end_node.bank_id AND rel.to_account_number = end_node.account_number
    UNION ALL
    SELECT
        vp.start_id,
        concat(toString(end_node.bank_id), '|', toString(end_node.account_number)) as end_id,
        vp.hop_count + 1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat(vp.path_nodes, [concat(toString(end_node.bank_id), '|', toString(end_node.account_number))]) as path_nodes,
        arrayConcat(vp.path_edges, [rel.transfer_id]) as path_edges,
        vp.start_account_number as start_account_number,
        end_node.account_number as end_account_number
    FROM vlp_a_b vp
    JOIN db_composite_id.transfers AS rel ON vp.end_id = concat(toString(rel.from_bank_id), '|', toString(rel.from_account_number))
    JOIN db_composite_id.accounts AS end_node ON rel.to_bank_id = end_node.bank_id AND rel.to_account_number = end_node.account_number
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, rel.transfer_id)
)
SELECT 
      t.start_account_number AS "a.account_number", 
      t.end_account_number AS "b.account_number"
FROM vlp_a_b AS t
