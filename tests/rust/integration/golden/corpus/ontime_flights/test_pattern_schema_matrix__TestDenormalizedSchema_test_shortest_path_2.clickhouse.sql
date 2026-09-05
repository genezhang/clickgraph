WITH RECURSIVE vlp_a_b_inner AS (
    SELECT
        t0.Origin as start_id,
        t0.Dest as end_id,
        1 as hop_count,
        [t0.Origin] as path_edges,
        [t0.Origin, t0.Dest] as path_nodes,
        ['FLIGHT'] as path_relationships,
        t0."OriginCityName" as "start_OriginCityName",
        t0."DestCityName" as "end_DestCityName",
        t0."OriginCityName" as "start_OriginCityName",
        t0."DestCityName" as "end_DestCityName"
    FROM default.flights AS t0
    WHERE 1 <= 5
    UNION ALL
    SELECT
        vp.start_id as start_id,
        next.Dest as end_id,
        vp.hop_count + 1,
        arrayConcat(vp.path_edges, [next.Origin]),
        arrayConcat(vp.path_nodes, [next.Dest]),
        arrayConcat(vp.path_relationships, ['FLIGHT']) as path_relationships,
        vp."start_OriginCityName" as "start_OriginCityName",
        next."DestCityName" as "end_DestCityName",
        vp."start_OriginCityName" as "start_OriginCityName",
        next."DestCityName" as "end_DestCityName"
    FROM vlp_a_b_inner vp
    JOIN default.flights next ON next.Origin = vp.end_id
    WHERE vp.hop_count < 5 AND NOT has(vp.path_nodes, next.Dest)
),
vlp_a_b AS (
    SELECT * FROM vlp_a_b_inner WHERE (start_OriginCityName != end_DestCityName)
)
SELECT 
      t.hop_count AS "length(p)"
FROM vlp_a_b AS t
LIMIT 5