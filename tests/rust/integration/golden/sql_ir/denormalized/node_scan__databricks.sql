WITH __multi_label_union AS (
SELECT 'Airport' as _label, string(code) as _id, to_json(struct(flights_denorm.code AS code)) as _properties FROM db_denormalized.flights_denorm
)
SELECT 
      n._label AS `n_label`, 
      n._id AS `n_id`, 
      n._properties AS `n_properties`
FROM __multi_label_union AS n
