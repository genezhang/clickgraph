SELECT 
      v0.full_name AS `u.name`, 
      v2.full_name AS `f.name`
FROM db_polymorphic.users AS v0
JOIN db_polymorphic.interactions AS v1 ON v1.from_id = v0.user_id
JOIN db_polymorphic.users AS v2 ON v1.to_id = v2.user_id
WHERE ((((v1.`interaction_type` = 'FOLLOWS') AND (v1.`from_type` = 'User')) AND (v1.`to_type` = 'User')))

-- column u.name: Value [("u.name", "u.name")]
-- column f.name: Value [("f.name", "f.name")]