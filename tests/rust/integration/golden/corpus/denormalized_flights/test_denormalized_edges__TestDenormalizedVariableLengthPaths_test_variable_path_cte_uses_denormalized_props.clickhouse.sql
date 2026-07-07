WITH RECURSIVE vlp_origin_dest AS (
    SELECT
        f.origin_code as start_id,
        f.dest_code as end_id,
        1 as hop_count,
        [f.origin_code] as path_edges,
        [f.origin_code, f.dest_code] as path_nodes,
        f.dest_city as end_dest_city
    FROM db_denormalized.flights_denorm AS f
    WHERE f.origin_city = 'Los Angeles' AND hop_count <= 2
    UNION ALL
    SELECT
        next.origin_code as start_id,
        next.dest_code as end_id,
        vp.hop_count + 1,
        arrayConcat(vp.path_edges, [next.origin_code]),
        arrayConcat(vp.path_nodes, [next.dest_code]),
        next.dest_city as end_dest_city
    FROM vlp_origin_dest vp
    JOIN db_denormalized.flights_denorm next ON next.origin_code = vp.end_id
    WHERE vp.hop_count < 2 AND NOT has(vp.path_nodes, next.dest_code)
)
SELECT 
      t.end_dest_city AS "dest.city", 
      count(*) AS "path_count"
FROM vlp_origin_dest AS t
GROUP BY t.end_dest_city
