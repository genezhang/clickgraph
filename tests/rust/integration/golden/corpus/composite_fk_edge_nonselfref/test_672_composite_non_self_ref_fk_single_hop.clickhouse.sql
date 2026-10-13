SELECT 
      v0.comment_id AS "c.comment_id", 
      v2.forum_id AS "f.forum_id"
FROM db_composite_fk.comments_composite AS v0
JOIN db_composite_fk.comments_composite AS v1 ON v1.comment_id = v0.comment_id
JOIN db_composite_fk.forums_composite AS v2 ON v1.forum_region = v2.region AND v1.forum_id = v2.forum_id
LIMIT 5