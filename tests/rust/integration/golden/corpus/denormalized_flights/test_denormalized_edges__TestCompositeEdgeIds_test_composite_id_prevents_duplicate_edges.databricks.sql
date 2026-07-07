WITH RECURSIVE vlp_origin_dest_inner AS (
    SELECT
        f.origin_code as start_id,
        f.dest_code as end_id,
        1 as hop_count,
        array(f.origin_code) as path_edges,
        array(f.origin_code, f.dest_code) as path_nodes,
        f.dest_code as end_dest_code
    FROM db_denormalized.flights_denorm AS f
    WHERE f.origin_code = 'LAX' AND hop_count <= 2
    UNION ALL
    SELECT
        next.origin_code as start_id,
        next.dest_code as end_id,
        vp.hop_count + 1,
        concat(vp.path_edges, array(next.origin_code)),
        concat(vp.path_nodes, array(next.dest_code)),
        next.dest_code as end_dest_code
    FROM vlp_origin_dest_inner vp
    JOIN db_denormalized.flights_denorm next ON next.origin_code = vp.end_id
    WHERE vp.hop_count < 2 AND NOT array_contains(vp.path_nodes, next.dest_code)
),
vlp_origin_dest AS (
    SELECT * FROM vlp_origin_dest_inner WHERE end_dest_code = 'ATL' AND hop_count >= 2
)
SELECT 
      t.hop_count AS `hops`
FROM vlp_origin_dest AS t
LIMIT 1