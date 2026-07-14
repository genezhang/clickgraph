WITH RECURSIVE vlp_a_dest AS (
    SELECT 
        concat(string(start_node.bank_id), '|', string(start_node.account_number)) as start_id,
        concat(string(end_node.bank_id), '|', string(end_node.account_number)) as end_id,
        1 as hop_count,
        array('TRANSFERRED') as path_relationships,
        array(concat(string(start_node.bank_id), '|', string(start_node.account_number)), concat(string(end_node.bank_id), '|', string(end_node.account_number))) as path_nodes,
        array(rel.transfer_id) as path_edges,
        end_node.bank_id as end_bank_id,
        end_node.account_number as end_account_number
    FROM db_composite_id.accounts AS start_node
    JOIN db_composite_id.transfers AS rel ON start_node.bank_id = rel.from_bank_id AND start_node.account_number = rel.from_account_number
    JOIN db_composite_id.accounts AS end_node ON rel.to_bank_id = end_node.bank_id AND rel.to_account_number = end_node.account_number
    WHERE (start_node.bank_id = 'B001' AND start_node.account_number = 'ACC001')
    UNION ALL
    SELECT
        vp.start_id,
        concat(string(end_node.bank_id), '|', string(end_node.account_number)) as end_id,
        vp.hop_count + 1 as hop_count,
        concat(vp.path_relationships, array('TRANSFERRED')) as path_relationships,
        concat(vp.path_nodes, array(concat(string(end_node.bank_id), '|', string(end_node.account_number)))) as path_nodes,
        concat(vp.path_edges, array(rel.transfer_id)) as path_edges,
        end_node.bank_id as end_bank_id,
        end_node.account_number as end_account_number
    FROM vlp_a_dest vp
    JOIN db_composite_id.transfers AS rel ON vp.end_id = concat(string(rel.from_bank_id), '|', string(rel.from_account_number))
    JOIN db_composite_id.accounts AS end_node ON rel.to_bank_id = end_node.bank_id AND rel.to_account_number = end_node.account_number
    WHERE vp.hop_count < 2
      AND NOT array_contains(vp.path_edges, rel.transfer_id)
)
SELECT 
      t.hop_count AS `length(p)`, 
      t.end_bank_id AS `dest.bank_id`, 
      t.end_account_number AS `dest.account_number`
FROM vlp_a_dest AS t
LIMIT 10