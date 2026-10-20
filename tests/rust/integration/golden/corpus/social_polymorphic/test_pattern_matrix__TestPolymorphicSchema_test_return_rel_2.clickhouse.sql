SELECT 
      v1.from_id AS "r.from_id", 
      v1.to_id AS "r.to_id", 
      v1.from_id AS "r.edge_id_1", 
      v1.to_id AS "r.edge_id_2", 
      v1.interaction_type AS "r.edge_id_3", 
      v1.timestamp AS "r.edge_id_4", 
      v1.timestamp AS "r.created_at", 
      v1.interaction_weight AS "r.weight"
FROM brahmand.users_bench AS v0
JOIN brahmand.interactions AS v1 ON v1.from_id = v0.user_id
JOIN brahmand.users_bench AS v2 ON v1.to_id = v2.user_id
WHERE ((((v1."interaction_type" = 'AUTHORED') AND (v1."from_type" = 'User')) AND (v1."to_type" = 'User')))
LIMIT 5
-- column r: Rel { rel_type: "AUTHORED", from_label: "User", to_label: "User" } [("from_id", "r.from_id"), ("to_id", "r.to_id"), ("edge_id_1", "r.edge_id_1"), ("edge_id_2", "r.edge_id_2"), ("edge_id_3", "r.edge_id_3"), ("edge_id_4", "r.edge_id_4"), ("created_at", "r.created_at"), ("weight", "r.weight")]