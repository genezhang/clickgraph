WITH RECURSIVE vlp_a_b AS (
    SELECT
        t0.origin_code as start_id,
        t0.dest_code as end_id,
        1 as hop_count,
        array(t0.origin_code) as path_edges,
        array(t0.origin_code, t0.dest_code) as path_nodes,
        array() as path_relationships,
        t0.`origin_code` as `start_origin_code`,
        t0.`dest_code` as `end_dest_code`
    FROM db_denormalized.flights_denorm AS t0
    WHERE 1 <= 2
    UNION ALL
    SELECT
        vp.start_id as start_id,
        next.dest_code as end_id,
        vp.hop_count + 1,
        concat(vp.path_edges, array(next.origin_code)),
        concat(vp.path_nodes, array(next.dest_code)),
        array() as path_relationships,
        vp.`start_origin_code` as `start_origin_code`,
        next.`dest_code` as `end_dest_code`
    FROM vlp_a_b vp
    JOIN db_denormalized.flights_denorm next ON next.origin_code = vp.end_id
    WHERE vp.hop_count < 2 AND NOT array_contains(vp.path_nodes, next.dest_code)
)
SELECT 
      t1.origin_code AS `x.code`, 
      t.start_origin_code AS `a.code`, 
      t.end_dest_code AS `b.code`
FROM vlp_a_b AS t
INNER JOIN db_denormalized.flights_denorm AS t1 ON t1.dest_code = t.start_id
