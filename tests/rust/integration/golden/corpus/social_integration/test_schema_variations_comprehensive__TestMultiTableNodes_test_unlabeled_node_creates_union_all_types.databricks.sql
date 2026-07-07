SELECT count(`n.post_id`) AS `total` FROM (
SELECT 
      NULL AS `age`,
      string(n.author_id) AS `author_id`,
      NULL AS `city`,
      string(n.post_content) AS `content`,
      NULL AS `country`,
      string(n.post_date) AS `created_at`,
      NULL AS `email`,
      NULL AS `is_active`,
      NULL AS `name`,
      string(n.post_id) AS `post_id`,
      NULL AS `registration_date`,
      string(n.post_title) AS `title`,
      NULL AS `user_id`,
      string(n.post_id) AS `n.post_id`
FROM test_integration.posts_test AS n
UNION ALL 
SELECT 
      string(n.age) AS `age`,
      NULL AS `author_id`,
      string(n.city) AS `city`,
      NULL AS `content`,
      string(n.country) AS `country`,
      NULL AS `created_at`,
      string(n.email_address) AS `email`,
      string(n.is_active) AS `is_active`,
      string(n.full_name) AS `name`,
      NULL AS `post_id`,
      string(n.registration_date) AS `registration_date`,
      NULL AS `title`,
      string(n.user_id) AS `user_id`,
      NULL AS `n.post_id`
FROM test_integration.users_test AS n
) AS __union
