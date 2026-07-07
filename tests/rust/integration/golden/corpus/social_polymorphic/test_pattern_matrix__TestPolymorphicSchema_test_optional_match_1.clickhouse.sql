SELECT `a.user_id` AS "a.user_id", count(*) AS "rel_count" FROM (
SELECT 
      toString(b.content) AS "content",
      toString(b.created_at) AS "created",
      toString(a.email_address) AS "email",
      toString(a.full_name) AS "name",
      toString(b.post_id) AS "post_id",
      toString(b.content) AS "title",
      toString(a.user_id) AS "user_id",
      a.user_id AS "a.user_id"
FROM brahmand.users_bench AS a
LEFT JOIN (SELECT * FROM brahmand.interactions WHERE (interaction_type = 'LIKES' AND from_type = 'User' AND to_type = 'Post')) AS r ON r.from_id = a.user_id
LEFT JOIN brahmand.posts_bench AS b ON b.post_id = r.to_id
UNION ALL 
SELECT 
      NULL AS "content",
      NULL AS "created",
      toString(b.email_address) AS "email",
      toString(b.full_name) AS "name",
      NULL AS "post_id",
      NULL AS "title",
      toString(b.user_id) AS "user_id",
      a.user_id AS "a.user_id"
FROM brahmand.users_bench AS a
LEFT JOIN (SELECT * FROM brahmand.interactions WHERE (interaction_type = 'LIKES' AND from_type = 'User' AND to_type = 'User')) AS r ON r.from_id = a.user_id
LEFT JOIN brahmand.users_bench AS b ON b.user_id = r.to_id
) AS __union
GROUP BY `a.user_id`
