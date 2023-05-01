MERGE {project_id}.{dataset_id}.{table_name} dst
USING (
SELECT
id,
related_post_id,
link_type_id,
creation_date,
MAX(staging_raw_id) AS staging_raw_id
FROM {project_id}.{dataset_id}.{src_table_name}
WHERE id IS NOT NULL
GROUP BY
id,
related_post_id,
link_type_id,
creation_date
) src
ON dst.id = src.id
AND dst.related_post_id = src.related_post_id
AND dst.link_type_id = src.link_type_id
WHEN NOT MATCHED THEN
INSERT (
id,
related_post_id,
link_type_id,
creation_date,
uuid,
ingestion_date
) VALUES (
src.id,
src.related_post_id,
src.link_type_id,
src.creation_date,
GENERATE_UUID(),
DATE(CURRENT_TIMESTAMP())
);