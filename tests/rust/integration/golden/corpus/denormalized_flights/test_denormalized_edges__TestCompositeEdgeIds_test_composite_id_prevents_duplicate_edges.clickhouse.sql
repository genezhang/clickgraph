WITH RECURSIVE vlp_origin_dest_inner AS (
    SELECT
        f.Origin as start_id,
        f.Dest as end_id,
        1 as hop_count,
        [tuple(f.flight_id, f.flight_number)] as path_edges,
        [f.Origin, f.Dest] as path_nodes,
        ['FLIGHT'] as path_relationships,
        f."Dest" as "end_Dest"
    FROM test_integration.flights AS f
    WHERE f.Origin = 'LAX' AND 1 <= 2
    UNION ALL
    SELECT
        vp.start_id as start_id,
        next.Dest as end_id,
        vp.hop_count + 1,
        arrayConcat(vp.path_edges, [tuple(next.flight_id, next.flight_number)]),
        arrayConcat(vp.path_nodes, [next.Dest]),
        arrayConcat(vp.path_relationships, ['FLIGHT']) as path_relationships,
        next."Dest" as "end_Dest"
    FROM vlp_origin_dest_inner vp
    JOIN test_integration.flights next ON next.Origin = vp.end_id
    WHERE vp.hop_count < 2 AND NOT has(vp.path_edges, tuple(next.flight_id, next.flight_number))
),
vlp_origin_dest AS (
    SELECT * FROM vlp_origin_dest_inner WHERE end_Dest = 'ATL' AND hop_count >= 2
)
SELECT 
      t.hop_count AS "hops"
FROM vlp_origin_dest AS t
LIMIT 1