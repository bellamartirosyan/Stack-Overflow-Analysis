MERGE {project_id}.{dataset_id}.{table_name} dst
USING (
SELECT
post_id,
user_id,
revision_guid,
creation_date,
text,
comment,
ingestion_date,
MAX(staging_raw_id) AS staging_raw_id
FROM
{project_id}.{dataset_id}.{src_table_name}
WHERE
post_id IS NOT NULL
AND user_id IS NOT NULL
AND revision_guid IS NOT NULL
AND creation_date IS NOT NULL
GROUP BY
post_id,
user_id,
revision_guid,
creation_date,
text,
comment,
ingestion_date
) src
ON dst.id_NK = src.post_id AND dst.revision_guid = src.revision_guid
WHEN NOT MATCHED THEN
INSERT (
id_NK,
post_id,
user_id,
revision_guid,
creation_date,
text,
comment,
uuid,
ingestion_date,
staging_raw_id
)
VALUES (
src.post_id,
src.post_id,
src.user_id,
src.revision_guid,
src.creation_date,
src.text,
src.comment,
Generate_uuid(),
src.ingestion_date,
src.staging_raw_id
)
WHEN MATCHED THEN
UPDATE SET
dst.text = src.text,
dst.comment = src.comment,
dst.staging_raw_id = src.staging_raw_id;