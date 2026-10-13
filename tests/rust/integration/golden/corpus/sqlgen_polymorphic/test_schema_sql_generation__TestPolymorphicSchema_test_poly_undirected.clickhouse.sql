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
)
SELECT 
      v0.full_name AS "u.name", 
      v2.full_name AS "f.name"
FROM db_polymorphic.users AS v0
JOIN v1_both AS v1 ON v1.__cg_start_0 = v0.user_id
JOIN db_polymorphic.users AS v2 ON v1.__cg_end_0 = v2.user_id
