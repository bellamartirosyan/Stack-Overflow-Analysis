MERGE {project_id}.{dataset_id}.{table_name} dst
USING (
SELECT
id,
display_name,
location,
reputation,
views,
up_votes,
down_votes,
creation_date,
last_access_date,
uuid,
ingestion_date,
staging_raw_id
FROM (
SELECT
*,
ROW_NUMBER() OVER (PARTITION BY id ORDER BY ingestion_date DESC) AS row_num
FROM {project_id}.{dataset_id}.{src_table_name}
)
WHERE row_num = 1 AND id IS NOT NULL
) src
ON dst.id = src.id AND dst.effective_to = TIMESTAMP("9999-12-31 23:59:59.999999 UTC")
WHEN NOT MATCHED THEN
INSERT (
districtid_sk,
districtname,
ingestion_date,
effective_from,
effective_to,
is_current,
staging_raw_id
)
VALUES(
Generate_uuid(),
src.display_name,
src.location,
src.reputation,
src.views,
src.up_votes,
src.down_votes,
src.creation_date,
src.last_access_date,
src.uuid,
src.ingestion_date,
CURRENT_TIMESTAMP(),
TIMESTAMP("9999-12-31 23:59:59.999999 UTC"),
TRUE,
src.staging_raw_id
)
WHEN MATCHED AND (
dst.display_name != src.display_name OR
dst.location != src.location OR
dst.reputation != src.reputation OR
dst.views != src.views OR
dst.up_votes != src.up_votes OR
dst.down_votes != src.down_votes OR
dst.creation_date != src.creation_date OR
dst.last_access_date != src.last_access_date OR
dst.uuid != src.uuid
) THEN
INSERT (
districtid_sk,
districtname,
ingestion_date,
effective_from,
effective_to,
is_current,
staging_raw_id
)
VALUES(
Generate_uuid(),
src.display_name,
src.location,
src.reputation,
src.views,
src.up_votes,
src.down_votes,
src.creation_date,
src.last_access_date,
src.uuid,
src.ingestion_date,
CURRENT_TIMESTAMP(),
TIMESTAMP("9999-12-31 23:59:59.999999 UTC"),
TRUE,
src.staging_raw_id
)
UPDATE SET dst.effective_to = CURRENT_TIMESTAMP(), dst.is_current = FALSE;