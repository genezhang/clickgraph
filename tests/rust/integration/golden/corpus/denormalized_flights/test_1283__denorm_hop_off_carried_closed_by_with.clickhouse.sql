WITH with_w1 AS (
SELECT 
      v1.Dest AS "v3__code", 
      v1.Dest AS "p2_v3_code"
FROM test_integration.flights AS v1
WHERE (v1.Origin IS NOT NULL AND v1.Dest IS NOT NULL)
), 
with_w2 AS (
SELECT 
      w1.v3__code AS "v6__code", 
      w1.p2_v3_code AS "p2_v6_code", 
      v4.Dest AS "v7__code", 
      v4.Dest AS "p2_v7_code"
FROM with_w1 AS w1
JOIN test_integration.flights AS v4 ON v4.Origin = w1.v3__code
WHERE v4.Dest IS NOT NULL
)
SELECT 
      w2.p2_v6_code AS "x", 
      w2.p2_v7_code AS "y"
FROM with_w2 AS w2
