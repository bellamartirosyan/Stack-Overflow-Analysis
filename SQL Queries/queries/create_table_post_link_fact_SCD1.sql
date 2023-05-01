CREATE TABLE IF NOT EXISTS {project_id}.{dataset_id}.{table_name}(
id INT64,
related_post_id INT64,
link_type_id INT64
creation_date TIMESTAMP,
uuid STRING,
ingestion_date DATE
);