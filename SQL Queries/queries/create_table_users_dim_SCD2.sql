CREATE TABLE IF NOT EXISTS {project_id}.{dataset_id}.{table_name} (
  id INT64,
  display_name STRING,
  location STRING,
  reputation INT64,
  views INT64,
  up_votes INT64,
  down_votes INT64,
  creation_date TIMESTAMP,
  last_access_date TIMESTAMP,
  uuid STRING,
  ingestion_date DATE,
  effective_from TIMESTAMP,
  effective_to TIMESTAMP,
  is_current BOOLEAN
);
