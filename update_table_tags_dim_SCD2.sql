-- Script to load data into tags SCD type 2
MERGE {project_id}.{dataset_id}.{table_name} dst
USING (
SELECT
id,
tag_name,
ingestion_date,
Max(staging_raw_id) AS staging_raw_id
FROM
{project_id}.{dataset_id}.{src_table_name}
WHERE
id IS NOT NULL
AND tag_name IS NOT NULL
GROUP BY
id,
tag_name,
ingestion_date
) src
ON dst.id = src.id AND dst.is_current = true
WHEN MATCHED AND dst.tag_name <> src.tag_name THEN
UPDATE SET dst.is_current = false, dst.end_date = src.ingestion_date
WHEN NOT MATCHED THEN
INSERT (
id,
tag_name,
uuid,
start_date,
end_date,
ingestion_date,
is_current
)
VALUES (
src.id,
src.tag_name,
GENERATE_UUID(),
src.ingestion_date,
CAST('9999-12-31' AS TIMESTAMP),
src.ingestion_date,
true
);