SELECT `a.name` AS `a.name`, count(`r.from_id`) AS `rel_count` FROM (
SELECT 
      string(b.content) AS `content`,
      string(b.created_at) AS `created`,
      string(a.email_address) AS `email`,
      string(a.full_name) AS `name`,
      string(b.post_id) AS `post_id`,
      string(b.content) AS `title`,
      string(a.user_id) AS `user_id`,
      a.full_name AS `a.name`,
      r.from_id AS `r.from_id`
FROM brahmand.users_bench AS a
LEFT JOIN (SELECT * FROM brahmand.interactions WHERE (interaction_type = 'LIKES' AND from_type = 'User' AND to_type = 'Post')) AS r ON r.from_id = a.user_id
LEFT JOIN brahmand.posts_bench AS b ON b.post_id = r.to_id
UNION ALL 
SELECT 
      NULL AS `content`,
      NULL AS `created`,
      string(b.email_address) AS `email`,
      string(b.full_name) AS `name`,
      NULL AS `post_id`,
      NULL AS `title`,
      string(b.user_id) AS `user_id`,
      a.full_name AS `a.name`,
      r.from_id AS `r.from_id`
FROM brahmand.users_bench AS a
LEFT JOIN (SELECT * FROM brahmand.interactions WHERE (interaction_type = 'LIKES' AND from_type = 'User' AND to_type = 'User')) AS r ON r.from_id = a.user_id
LEFT JOIN brahmand.users_bench AS b ON b.user_id = r.to_id
) AS __union
GROUP BY `a.name`
