SELECT 
      c.comment_id AS `c.comment_id`, 
      f.forum_id AS `f.forum_id`
FROM db_composite_fk.comments_composite AS c
INNER JOIN db_composite_fk.forums_composite AS f ON f.region = c.forum_region AND f.forum_id = c.forum_id
LIMIT 5