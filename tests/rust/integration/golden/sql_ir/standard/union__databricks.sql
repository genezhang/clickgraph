WITH __multi_label_union AS (
SELECT 'Post' as _label, string(post_id) as _id, to_json(struct(posts_bench.author_id AS author_id, posts_bench.post_content AS content, posts_bench.post_date AS date, posts_bench.post_id AS post_id, posts_bench.post_title AS title)) as _properties FROM social.posts_bench
UNION ALL
SELECT 'User' as _label, string(user_id) as _id, to_json(struct(users_bench.city AS city, users_bench.country AS country, users_bench.email_address AS email, users_bench.is_active AS is_active, users_bench.full_name AS name, users_bench.registration_date AS registration_date, users_bench.user_id AS user_id)) as _properties FROM social.users_bench
)
SELECT 
      n._label AS `n_label`, 
      n._id AS `n_id`, 
      n._properties AS `n_properties`
FROM __multi_label_union AS n
