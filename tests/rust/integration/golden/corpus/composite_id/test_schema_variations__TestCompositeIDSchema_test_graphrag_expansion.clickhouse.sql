WITH RECURSIVE vlp_a_dest AS (
    SELECT 
        concat(toString(start_node.bank_id), '|', toString(start_node.account_number)) as start_id,
        concat(toString(end_node.bank_id), '|', toString(end_node.account_number)) as end_id,
        1 as hop_count,
        ['TRANSFERRED'] as path_relationships,
        [concat(toString(start_node.bank_id), '|', toString(start_node.account_number)), concat(toString(end_node.bank_id), '|', toString(end_node.account_number))] as path_nodes,
        end_node.bank_id as end_bank_id,
        end_node.account_number as end_account_number
    FROM db_composite_id.accounts AS start_node
    JOIN db_composite_id.transfers AS rel ON start_node.bank_id = rel.from_bank_id AND start_node.account_number = rel.from_account_number
    JOIN db_composite_id.accounts AS end_node ON rel.to_bank_id = end_node.bank_id AND rel.to_account_number = end_node.account_number
    WHERE (start_node.bank_id = 'B001' AND start_node.account_number = 'ACC001')
    UNION ALL
    SELECT
        vp.start_id,
        concat(toString(end_node.bank_id), '|', toString(end_node.account_number)) as end_id,
        vp.hop_count + 1 as hop_count,
        arrayConcat(vp.path_relationships, ['TRANSFERRED']) as path_relationships,
        arrayConcat(vp.path_nodes, [concat(toString(end_node.bank_id), '|', toString(end_node.account_number))]) as path_nodes,
        end_node.bank_id as end_bank_id,
        end_node.account_number as end_account_number
    FROM vlp_a_dest vp
    JOIN db_composite_id.transfers AS rel ON vp.end_id = concat(toString(rel.from_bank_id), '|', toString(rel.from_account_number))
    JOIN db_composite_id.accounts AS end_node ON rel.to_bank_id = end_node.bank_id AND rel.to_account_number = end_node.account_number
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_nodes, concat(toString(end_node.bank_id), '|', toString(end_node.account_number)))
)
SELECT 
      t.hop_count AS "length(p)", 
      t.end_bank_id AS "dest.bank_id", 
      t.end_account_number AS "dest.account_number"
FROM vlp_a_dest AS t
LIMIT 10