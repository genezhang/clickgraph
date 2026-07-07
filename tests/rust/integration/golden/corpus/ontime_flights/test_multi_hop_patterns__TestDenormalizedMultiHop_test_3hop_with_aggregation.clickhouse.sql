WITH with_connections_hub_cte_0 AS (SELECT 
      f2.Origin AS "hub", 
      count(*) AS "connections"
FROM default.flights AS f1
INNER JOIN default.flights AS f2 ON f2.Origin = f1.Dest
INNER JOIN default.flights AS f3 ON f3.Origin = f2.Dest
GROUP BY f2.Origin
)
SELECT 
      connections_hub.hub AS "hub", 
      connections_hub.connections AS "connections"
FROM with_connections_hub_cte_0 AS connections_hub
ORDER BY connections_hub.connections DESC
LIMIT 5