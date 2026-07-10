SELECT 
      author.full_name AS "author.name", 
      liker.full_name AS "liker.name", 
      p.content AS "p.content"
FROM db_polymorphic.users AS author
INNER JOIN db_polymorphic.interactions AS t0 ON t0.from_id = author.user_id AND t0.interaction_type = 'AUTHORED' AND t0.from_type = 'User' AND t0.to_type = 'Post'
INNER JOIN db_polymorphic.posts AS p ON p.post_id = t0.to_id
INNER JOIN db_polymorphic.interactions AS t1 ON t1.to_id = p.post_id AND t1.interaction_type = 'LIKES' AND t1.from_type = 'User' AND t1.to_type = 'Post'
INNER JOIN db_polymorphic.users AS liker ON liker.user_id = t1.from_id
