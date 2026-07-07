WITH RECURSIVE vlp_a1_a2 AS (
    SELECT 
        concat(toString(start_node.bank_id), '|', toString(start_node.account_number)) as start_id,
        concat(toString(end_node.bank_id), '|', toString(end_node.account_number)) as end_id,
        1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [concat(toString(start_node.bank_id), '|', toString(start_node.account_number)), concat(toString(end_node.bank_id), '|', toString(end_node.account_number))] as path_nodes,
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
        end_node.account_number as end_account_number
    FROM vlp_a1_a2 vp
    JOIN db_composite_id.transfers AS rel ON vp.end_id = concat(toString(rel.from_bank_id), '|', toString(rel.from_account_number))
    JOIN db_composite_id.accounts AS end_node ON rel.to_bank_id = end_node.bank_id AND rel.to_account_number = end_node.account_number
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_nodes, concat(toString(end_node.bank_id), '|', toString(end_node.account_number)))
)
SELECT 
      t.end_account_number AS "a2.account_number"
FROM vlp_a1_a2 AS t
