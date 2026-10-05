WITH RECURSIVE vlp_a_b AS (
    SELECT
        t0.Origin as start_id,
        t0.Dest as end_id,
        1 as hop_count,
        [tuple(t0.flight_id, t0.flight_number)] as path_edges,
        [t0.Origin, t0.Dest] as path_nodes,
        [] as path_relationships,
        t0."OriginCityName" as "start_OriginCityName",
        t0."Origin" as "start_Origin",
        t0."OriginState" as "start_OriginState",
        t0."DestCityName" as "end_DestCityName",
        t0."Dest" as "end_Dest",
        t0."DestState" as "end_DestState"
    FROM test_integration.flights AS t0
    WHERE 1 <= 2
    UNION ALL
    SELECT
        vp.start_id as start_id,
        next.Dest as end_id,
        vp.hop_count + 1,
        arrayConcat(vp.path_edges, [tuple(next.flight_id, next.flight_number)]),
        arrayConcat(vp.path_nodes, [next.Dest]),
        [] as path_relationships,
        vp."start_OriginCityName" as "start_OriginCityName",
        vp."start_Origin" as "start_Origin",
        vp."start_OriginState" as "start_OriginState",
        next."DestCityName" as "end_DestCityName",
        next."Dest" as "end_Dest",
        next."DestState" as "end_DestState"
    FROM vlp_a_b vp
    JOIN test_integration.flights next ON next.Origin = vp.end_id
    WHERE vp.hop_count < 2 AND NOT has(vp.path_edges, tuple(next.flight_id, next.flight_number))
), 
vlp_b_c AS (
    SELECT
        t1.Origin as start_id,
        t1.Dest as end_id,
        1 as hop_count,
        [tuple(t1.flight_id, t1.flight_number)] as path_edges,
        [t1.Origin, t1.Dest] as path_nodes,
        [] as path_relationships,
        t1."OriginCityName" as "start_OriginCityName",
        t1."Origin" as "start_Origin",
        t1."OriginState" as "start_OriginState",
        t1."DestCityName" as "end_DestCityName",
        t1."Dest" as "end_Dest",
        t1."DestState" as "end_DestState"
    FROM test_integration.flights AS t1
    WHERE 1 <= 2
    UNION ALL
    SELECT
        vp.start_id as start_id,
        next.Dest as end_id,
        vp.hop_count + 1,
        arrayConcat(vp.path_edges, [tuple(next.flight_id, next.flight_number)]),
        arrayConcat(vp.path_nodes, [next.Dest]),
        [] as path_relationships,
        vp."start_OriginCityName" as "start_OriginCityName",
        vp."start_Origin" as "start_Origin",
        vp."start_OriginState" as "start_OriginState",
        next."DestCityName" as "end_DestCityName",
        next."Dest" as "end_Dest",
        next."DestState" as "end_DestState"
    FROM vlp_b_c vp
    JOIN test_integration.flights next ON next.Origin = vp.end_id
    WHERE vp.hop_count < 2 AND NOT has(vp.path_edges, tuple(next.flight_id, next.flight_number))
)
SELECT 
      count(*) AS "count(*)"
FROM vlp_a_b AS t
INNER JOIN vlp_b_c AS t_ch_0 ON t_ch_0.start_id = t.end_id AND NOT hasAny(t_ch_0.path_edges, t.path_edges)
INNER JOIN test_integration.flights AS t2 ON t2.Origin = t_ch_0.end_id
