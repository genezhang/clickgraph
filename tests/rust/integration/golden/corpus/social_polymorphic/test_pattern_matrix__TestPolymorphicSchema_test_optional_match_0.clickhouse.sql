SELECT `a.email` AS "a.email", count(`r.from_id`) AS "rel_count" FROM (
SELECT 
      toString(a.email_address) AS "a.email",
      toString(a.full_name) AS "a.name",
      toString(a.user_id) AS "a.user_id",
      NULL AS "b.email",
      NULL AS "b.name",
      NULL AS "b.user_id",
      toString(b.content) AS "content",
      toString(b.created_at) AS "created",
      toString(a.email_address) AS "email",
      toString(a.full_name) AS "name",
      toString(b.post_id) AS "post_id",
      toString(b.content) AS "title",
      toString(a.user_id) AS "user_id",
      a.email_address AS "a.email_address",
      r.from_id AS "r.from_id"
FROM brahmand.users_bench AS a
LEFT JOIN (SELECT * FROM brahmand.interactions WHERE (interaction_type = 'FOLLOWS' AND from_type = 'User' AND to_type = 'Post')) AS r ON r.from_id = a.user_id
LEFT JOIN brahmand.posts_bench AS b ON b.post_id = r.to_id
UNION ALL 
SELECT 
      toString(a.email_address) AS "a.email",
      toString(a.full_name) AS "a.name",
      toString(a.user_id) AS "a.user_id",
      toString(b.email_address) AS "b.email",
      toString(b.full_name) AS "b.name",
      toString(b.user_id) AS "b.user_id",
      NULL AS "content",
      NULL AS "created",
      toString(b.email_address) AS "email",
      toString(b.full_name) AS "name",
      NULL AS "post_id",
      NULL AS "title",
      toString(b.user_id) AS "user_id",
      a.email_address AS "a.email_address",
      r.from_id AS "r.from_id"
FROM brahmand.users_bench AS a
LEFT JOIN (SELECT * FROM brahmand.interactions WHERE (interaction_type = 'FOLLOWS' AND from_type = 'User' AND to_type = 'User')) AS r ON r.from_id = a.user_id
LEFT JOIN brahmand.users_bench AS b ON b.user_id = r.to_id
) AS __union
GROUP BY `a.email`
