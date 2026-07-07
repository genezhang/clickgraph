WITH vlp_multi_type_ip_target AS (
SELECT 'IP' AS end_type, n2.ip AS end_id, ip_1.ip AS start_id, 'IP' AS start_type, string(n2.id.orig_h) AS r_from_id, string(n2.id.resp_h) AS r_to_id, to_json(struct(n2.ip AS ip)) AS end_properties, to_json(struct(ip_1.ip)) AS start_properties, ip_1.ip AS start_ip, 1 AS hop_count, array('CONNECTED_TO') AS path_relationships, array(to_json(struct(n2.conn_state, n2.duration, n2.orig_bytes, n2.proto, n2.resp_bytes, n2.service, n2.ts, n2.uid))) AS rel_properties, array(string(ip_1.ip), string(n2.ip)) AS path_nodes
FROM zeek.conn_log ip_1
INNER JOIN zeek.conn_log n2 ON ip_1.ip = n2.`id.orig_h`
WHERE (ip_1.`id.orig_h` = '192.168.1.10')
UNION ALL
SELECT 'Domain' AS end_type, n2.name AS end_id, ip_1.ip AS start_id, 'IP' AS start_type, string(n2.id.orig_h) AS r_from_id, string(n2.query) AS r_to_id, to_json(struct(n2.name AS name)) AS end_properties, to_json(struct(ip_1.ip)) AS start_properties, ip_1.ip AS start_ip, 1 AS hop_count, array('DNS_REQUESTED') AS path_relationships, array(to_json(struct(n2.answers, n2.`id.resp_h`, n2.qtype_name, n2.rcode_name, n2.ts, n2.uid))) AS rel_properties, array(string(ip_1.ip), string(n2.name)) AS path_nodes
FROM zeek.conn_log ip_1
INNER JOIN zeek.dns_log n2 ON ip_1.ip = n2.`id.orig_h`
WHERE (ip_1.`id.orig_h` = '192.168.1.10')
)
SELECT 
      element_at(t.path_relationships, 1) AS `type(r)`, 
      t.end_properties AS `target.properties`, 
      t.end_id AS `target.id`, 
      t.end_type AS `target.__label__`
FROM vlp_multi_type_ip_target AS t
LIMIT 10