SELECT count(coalesce(`n.post_id`, `n.user_id`)) AS `count(n)` FROM (
SELECT 
      string(n.author_id) AS `author_id`,
      NULL AS `city`,
      string(n.post_content) AS `content`,
      NULL AS `country`,
      string(n.post_date) AS `date`,
      NULL AS `email`,
      NULL AS `is_active`,
      NULL AS `name`,
      string(n.post_id) AS `post_id`,
      NULL AS `registration_date`,
      string(n.post_title) AS `title`,
      NULL AS `user_id`,
      string(n.post_id) AS `n.post_id`,
      NULL AS `n.user_id`
FROM social.posts_bench AS n
UNION ALL 
SELECT 
      NULL AS `author_id`,
      string(n.city) AS `city`,
      NULL AS `content`,
      string(n.country) AS `country`,
      NULL AS `date`,
      string(n.email_address) AS `email`,
      string(n.is_active) AS `is_active`,
      string(n.full_name) AS `name`,
      NULL AS `post_id`,
      string(n.registration_date) AS `registration_date`,
      NULL AS `title`,
      string(n.user_id) AS `user_id`,
      NULL AS `n.post_id`,
      string(n.user_id) AS `n.user_id`
FROM social.users_bench AS n
) AS __union
