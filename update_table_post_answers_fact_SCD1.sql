MERGE {project_id}.{dataset_id}.{dst_table_name} dst
USING (
SELECT
id,
parent_id,
owner_user_id,
body,
comment_count,
creation_date,
last_activity_date,
score,
uuid,
ingestion_date,
Max(staging_raw_id) AS staging_raw_id
FROM {project_id}.{dataset_id}.{src_table_name}
WHERE id IS NOT NULL
GROUP BY
id,
parent_id,
owner_user_id,
body,
comment_count,
creation_date,
last_activity_date,
score,
uuid,
ingestion_date
) src
ON dst.id_NK = src.id
WHEN NOT MATCHED THEN
INSERT (
id_SK,
id_NK,
parent_id,
owner_user_id,
body,
comment_count,
creation_date,
last_activity_date,
score,
uuid,
ingestion_date,
staging_raw_id
)
VALUES (
Generate_uuid(),
src.id,
src.parent_id,
src.owner_user_id,
src.body,
src.comment_count,
src.creation_date,
src.last_activity_date,
src.score,
src.uuid,
src.ingestion_date,
src.staging_raw_id
);