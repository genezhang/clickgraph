WITH RECURSIVE vlp_a_b AS (
    SELECT 
        concat(string(start_node.bank_id), '|', string(start_node.account_number)) as start_id,
        concat(string(end_node.bank_id), '|', string(end_node.account_number)) as end_id,
        1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        array(concat(string(start_node.bank_id), '|', string(start_node.account_number)), concat(string(end_node.bank_id), '|', string(end_node.account_number))) as path_nodes,
        start_node.account_number as start_account_number,
        end_node.account_number as end_account_number
    FROM db_composite_id.accounts AS start_node
    JOIN db_composite_id.transfers AS rel ON start_node.bank_id = rel.from_bank_id AND start_node.account_number = rel.from_account_number
    JOIN db_composite_id.accounts AS end_node ON rel.to_bank_id = end_node.bank_id AND rel.to_account_number = end_node.account_number
    UNION ALL
    SELECT
        vp.start_id,
        concat(string(end_node.bank_id), '|', string(end_node.account_number)) as end_id,
        vp.hop_count + 1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        concat(vp.path_nodes, array(concat(string(end_node.bank_id), '|', string(end_node.account_number)))) as path_nodes,
        vp.start_account_number as start_account_number,
        end_node.account_number as end_account_number
    FROM vlp_a_b vp
    JOIN db_composite_id.transfers AS rel ON vp.end_id = concat(string(rel.from_bank_id), '|', string(rel.from_account_number))
    JOIN db_composite_id.accounts AS end_node ON rel.to_bank_id = end_node.bank_id AND rel.to_account_number = end_node.account_number
    WHERE vp.hop_count < 3
      AND NOT array_contains(vp.path_nodes, concat(string(end_node.bank_id), '|', string(end_node.account_number)))
)
SELECT 
      t.start_account_number AS `a.account_number`, 
      t.end_account_number AS `b.account_number`
FROM vlp_a_b AS t
