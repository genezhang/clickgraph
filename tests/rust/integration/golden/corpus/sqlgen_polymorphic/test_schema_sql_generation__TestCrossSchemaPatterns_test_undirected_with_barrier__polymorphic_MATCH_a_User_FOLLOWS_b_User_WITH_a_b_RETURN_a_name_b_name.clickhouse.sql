WITH v1_both AS (
SELECT 
      e.from_id AS "from_id", 
      e.interaction_type AS "interaction_type", 
      e.interaction_weight AS "interaction_weight", 
      e.timestamp AS "timestamp", 
      e.to_id AS "to_id", 
      e.from_id AS "__cg_start_0", 
      e.to_id AS "__cg_end_0"
FROM db_polymorphic.interactions AS e
WHERE ((((e."interaction_type" = 'FOLLOWS') AND (e."from_type" = 'User')) AND (e."to_type" = 'User')))

UNION ALL
SELECT 
      e.from_id AS "from_id", 
      e.interaction_type AS "interaction_type", 
      e.interaction_weight AS "interaction_weight", 
      e.timestamp AS "timestamp", 
      e.to_id AS "to_id", 
      e.to_id AS "__cg_start_0", 
      e.from_id AS "__cg_end_0"
FROM db_polymorphic.interactions AS e
WHERE (((((e."interaction_type" = 'FOLLOWS') AND (e."from_type" = 'User')) AND (e."to_type" = 'User'))) AND e.from_id <> e.to_id)
), 
with_w2 AS (
SELECT 
      v0.user_id AS "v3__user_id", 
      v0.full_name AS "p2_v3_name", 
      v2.user_id AS "v4__user_id", 
      v2.full_name AS "p2_v4_name"
FROM db_polymorphic.users AS v0
JOIN v1_both AS v1 ON v1.__cg_start_0 = v0.user_id
JOIN db_polymorphic.users AS v2 ON v1.__cg_end_0 = v2.user_id
)
SELECT 
      w2.p2_v3_name AS "a.name", 
      w2.p2_v4_name AS "b.name"
FROM with_w2 AS w2

-- column a.name: Value [("a.name", "a.name")]
-- column b.name: Value [("b.name", "b.name")]