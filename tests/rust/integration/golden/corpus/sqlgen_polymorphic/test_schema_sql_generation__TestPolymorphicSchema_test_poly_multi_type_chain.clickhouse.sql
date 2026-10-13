SELECT 
      v0.full_name AS "author.name", 
      v4.full_name AS "liker.name", 
      v2.content AS "p.content"
FROM db_polymorphic.users AS v0
JOIN db_polymorphic.interactions AS v1 ON v1.from_id = v0.user_id
JOIN db_polymorphic.posts AS v2 ON v1.to_id = v2.post_id
JOIN db_polymorphic.interactions AS v3 ON v3.to_id = v2.post_id
JOIN db_polymorphic.users AS v4 ON v3.from_id = v4.user_id
WHERE (((((v1."interaction_type" = 'AUTHORED') AND (v1."from_type" = 'User')) AND (v1."to_type" = 'Post'))) AND ((((v3."interaction_type" = 'LIKES') AND (v3."from_type" = 'User')) AND (v3."to_type" = 'Post'))))
