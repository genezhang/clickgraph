SELECT 
      v0.full_name AS `u.name`, 
      count(v2.post_id) AS `post_count`
FROM db_standard.users AS v0
JOIN db_standard.posts AS v1 ON v1.user_id = v0.user_id
JOIN db_standard.posts AS v2 ON v1.post_id = v2.post_id
GROUP BY v0.full_name

-- column u.name: Value [("u.name", "u.name")]
-- column post_count: Value [("post_count", "post_count")]