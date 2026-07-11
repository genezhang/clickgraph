WITH RECURSIVE vlp_a_b AS (
    SELECT
        t0.Origin as start_id,
        t0.Dest as end_id,
        1 as hop_count,
        [t0.Origin] as path_edges,
        [t0.Origin, t0.Dest] as path_nodes,
        [] as path_relationships,
        t0.OriginState as start_OriginState,
        t0.DestState as end_DestState
    FROM default.flights AS t0
    WHERE 1 <= 3
    UNION ALL
    SELECT
        vp.start_id as start_id,
        next.Dest as end_id,
        vp.hop_count + 1,
        arrayConcat(vp.path_edges, [next.Origin]),
        arrayConcat(vp.path_nodes, [next.Dest]),
        [] as path_relationships,
        vp.start_OriginState as start_OriginState,
        next.DestState as end_DestState
    FROM vlp_a_b vp
    JOIN default.flights next ON next.Origin = vp.end_id
    WHERE vp.hop_count < 3 AND NOT has(vp.path_nodes, next.Dest)
)
SELECT 
      t.start_OriginState AS "a.state", 
      t.end_DestState AS "b.state"
FROM vlp_a_b AS t
LIMIT 10