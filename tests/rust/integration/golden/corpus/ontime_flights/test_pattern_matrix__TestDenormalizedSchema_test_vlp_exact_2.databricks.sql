WITH RECURSIVE vlp_a_b_inner AS (
    SELECT
        t0.Origin as start_id,
        t0.Dest as end_id,
        1 as hop_count,
        array(t0.Origin) as path_edges,
        array(t0.Origin, t0.Dest) as path_nodes,
        t0.OriginState as start_OriginState,
        t0.DestState as end_DestState
    FROM default.flights AS t0
    WHERE hop_count <= 2
    UNION ALL
    SELECT
        next.Origin as start_id,
        next.Dest as end_id,
        vp.hop_count + 1,
        concat(vp.path_edges, array(next.Origin)),
        concat(vp.path_nodes, array(next.Dest)),
        vp.start_OriginState as start_OriginState,
        next.DestState as end_DestState
    FROM vlp_a_b_inner vp
    JOIN default.flights next ON next.Origin = vp.end_id
    WHERE vp.hop_count < 2 AND NOT array_contains(vp.path_nodes, next.Dest)
),
vlp_a_b AS (
    SELECT * FROM vlp_a_b_inner WHERE hop_count >= 2
)
SELECT 
      t.start_OriginState AS `a.state`, 
      t.end_DestState AS `b.state`
FROM vlp_a_b AS t
LIMIT 10