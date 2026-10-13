WITH RECURSIVE vlp_v1_nodes AS (
SELECT 
      'Customer' AS "__cg_label", 
      e.customer_id AS "__cg_id_0", 
      concat('Customer', ':', toString(e.customer_id)) AS "__cg_key"
FROM test_integration.customers_fk AS e

UNION ALL
SELECT 
      'Order' AS "__cg_label", 
      e.order_id AS "__cg_id_0", 
      concat('Order', ':', toString(e.order_id)) AS "__cg_key"
FROM test_integration.orders_fk AS e
), 
v1_rels AS (
SELECT 
      'PLACED_BY' AS "__cg_type", 
      'Order' AS "__cg_from_label", 
      'Customer' AS "__cg_to_label", 
      e.order_id AS "__cg_rid_0", 
      e.customer_id AS "__cg_rid_1", 
      e.order_id AS "__cg_from_0", 
      e.customer_id AS "__cg_to_0", 
      e.customer_id AS "__cg_start_0", 
      e.order_id AS "__cg_end_0", 
      'Customer' AS "__cg_start_label", 
      'Order' AS "__cg_end_label", 
      concat('PLACED_BY:Order:Customer:', toString(tuple(e.order_id, e.customer_id))) AS "__cg_key"
FROM test_integration.orders_fk AS e
), 
vlp_v1_trails AS (
    SELECT start_node.__cg_label AS start_label, start_node.__cg_id_0 AS start_id, start_node.__cg_label AS end_label, start_node.__cg_id_0 AS end_id, CAST(0 AS UInt32) AS hop_count, CAST([], 'Array(String)') AS path_edges, [start_node.__cg_key] AS path_nodes
    FROM vlp_v1_nodes AS start_node
    WHERE (start_node.__cg_label = 'Customer') AND ((start_node.__cg_label, start_node.__cg_id_0) IN (SELECT DISTINCT 
      'Customer' AS "label", 
      v0.customer_id AS "id"
FROM test_integration.customers_fk AS v0
WHERE v0.customer_id = 1))
    UNION ALL
    SELECT vp.start_label AS start_label, vp.start_id AS start_id, end_node.__cg_label AS end_label, end_node.__cg_id_0 AS end_id, CAST(vp.hop_count + 1 AS UInt32) AS hop_count, arrayConcat(vp.path_edges, [rel.__cg_key]) AS path_edges, arrayConcat(vp.path_nodes, [end_node.__cg_key]) AS path_nodes
    FROM vlp_v1_trails AS vp
    JOIN v1_rels AS rel ON rel.__cg_start_label = vp.end_label AND rel.__cg_start_0 = vp.end_id
    JOIN vlp_v1_nodes AS end_node ON end_node.__cg_label = rel.__cg_end_label AND end_node.__cg_id_0 = rel.__cg_end_0
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, rel.__cg_key)
), 
vlp_v1_path AS (
    SELECT start_label, start_id, end_label, end_id, hop_count, path_edges, path_nodes
    FROM vlp_v1_trails
    WHERE hop_count >= 1
)
SELECT DISTINCT 
      v2.order_id AS "o.order_id", 
      v2.total_amount AS "o.total_amount"
FROM vlp_v1_path AS v1
JOIN test_integration.customers_fk AS v0 ON v1.start_id = v0.customer_id
JOIN test_integration.orders_fk AS v2 ON v1.end_id = v2.order_id
WHERE (v1.end_label = 'Order' AND v0.customer_id = 1)
LIMIT 10
-- column o.order_id: Value [("o.order_id", "o.order_id")]
-- column o.total_amount: Value [("o.total_amount", "o.total_amount")]