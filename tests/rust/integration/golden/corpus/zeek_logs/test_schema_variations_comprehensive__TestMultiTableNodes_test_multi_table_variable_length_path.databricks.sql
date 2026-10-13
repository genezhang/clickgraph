WITH RECURSIVE vlp_v1_nodes AS (
SELECT 
      'Domain' AS `__cg_label`, 
      e.query AS `__cg_id_0`, 
      concat('Domain', ':', CAST(e.query AS STRING)) AS `__cg_key`
FROM zeek.dns_log AS e

UNION ALL
SELECT 
      'IP' AS `__cg_label`, 
      e.`id.orig_h` AS `__cg_id_0`, 
      concat('IP', ':', CAST(e.`id.orig_h` AS STRING)) AS `__cg_key`
FROM zeek.dns_log AS e
), 
v1_rels AS (
SELECT 
      'DNS_REQUESTED' AS `__cg_type`, 
      'IP' AS `__cg_from_label`, 
      'Domain' AS `__cg_to_label`, 
      e.`id.orig_h` AS `__cg_rid_0`, 
      e.query AS `__cg_rid_1`, 
      e.`id.orig_h` AS `__cg_from_0`, 
      e.query AS `__cg_to_0`, 
      e.`id.orig_h` AS `__cg_start_0`, 
      e.query AS `__cg_end_0`, 
      'IP' AS `__cg_start_label`, 
      'Domain' AS `__cg_end_label`, 
      concat('DNS_REQUESTED:IP:Domain:', to_json(named_struct('_1', CAST(e.`id.orig_h` AS STRING), '_2', CAST(e.query AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM zeek.dns_log AS e
), 
vlp_v1_trails AS (
    SELECT start_node.__cg_label AS start_label, start_node.__cg_id_0 AS start_id, start_node.__cg_label AS end_label, start_node.__cg_id_0 AS end_id, CAST(0 AS INT) AS hop_count, CAST(array() AS ARRAY<STRING>) AS path_edges, array(start_node.__cg_key) AS path_nodes
    FROM vlp_v1_nodes AS start_node
    WHERE (start_node.__cg_label = 'IP')
    UNION ALL
    SELECT vp.start_label AS start_label, vp.start_id AS start_id, end_node.__cg_label AS end_label, end_node.__cg_id_0 AS end_id, CAST(vp.hop_count + 1 AS INT) AS hop_count, concat(vp.path_edges, array(rel.__cg_key)) AS path_edges, concat(vp.path_nodes, array(end_node.__cg_key)) AS path_nodes
    FROM vlp_v1_trails AS vp
    JOIN v1_rels AS rel ON rel.__cg_start_label = vp.end_label AND rel.__cg_start_0 = vp.end_id
    JOIN vlp_v1_nodes AS end_node ON end_node.__cg_label = rel.__cg_end_label AND end_node.__cg_id_0 = rel.__cg_end_0
    WHERE vp.hop_count < 2
      AND NOT array_contains(vp.path_edges, rel.__cg_key)
), 
vlp_v1_path AS (
    SELECT start_label, start_id, end_label, end_id, hop_count, path_edges, path_nodes
    FROM vlp_v1_trails
    WHERE hop_count >= 1
)
SELECT 
      count(*) AS `paths`
FROM vlp_v1_path AS v1
JOIN zeek.dns_log AS v0 ON v1.start_id = v0.`id.orig_h`
JOIN zeek.dns_log AS v2 ON v1.end_id = v2.query
WHERE v1.end_label = 'Domain'
