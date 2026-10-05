WITH RECURSIVE with_c_z_cte_0 AS (SELECT 
      c.account_number AS "p1_c_account_number", 
      c.bank_id AS "p1_c_bank_id", 
      z.account_number AS "p1_z_account_number", 
      z.bank_id AS "p1_z_bank_id"
FROM db_composite_id.accounts AS z
INNER JOIN db_composite_id.transfers AS t0 ON t0.from_bank_id = z.bank_id AND t0.from_account_number = z.account_number
INNER JOIN db_composite_id.accounts AS c ON c.bank_id = t0.to_bank_id AND c.account_number = t0.to_account_number
), 
vlp_z_c AS (
    SELECT 
        concat(toString(start_node.bank_id), '|', toString(start_node.account_number)) as start_id,
        concat(toString(end_node.bank_id), '|', toString(end_node.account_number)) as end_id,
        1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [concat(toString(start_node.bank_id), '|', toString(start_node.account_number)), concat(toString(end_node.bank_id), '|', toString(end_node.account_number))] as path_nodes,
        [rel.transfer_id] as path_edges
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
        arrayConcat(vp.path_edges, [rel.transfer_id]) as path_edges
    FROM vlp_z_c vp
    JOIN db_composite_id.transfers AS rel ON vp.end_id = concat(toString(rel.from_bank_id), '|', toString(rel.from_account_number))
    JOIN db_composite_id.accounts AS end_node ON rel.to_bank_id = end_node.bank_id AND rel.to_account_number = end_node.account_number
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, rel.transfer_id)
)
SELECT 
      c_z.p1_z_account_number AS "x", 
      c_z.p1_c_account_number AS "y"
FROM vlp_z_c AS t
INNER JOIN with_c_z_cte_0 AS c_z ON toString(t.end_id) = concat(toString(c_z.p1_c_bank_id), '|', toString(c_z.p1_c_account_number)) AND toString(t.start_id) = concat(toString(c_z.p1_z_bank_id), '|', toString(c_z.p1_z_account_number))
