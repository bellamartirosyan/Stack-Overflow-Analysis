CREATE TABLE IF NOT EXISTS {project_id}.{dataset_id}.{table_name} (
    id INT64 NOT NULL,
    tag_name STRING NOT NULL,
    uuid STRING NOT NULL,
    start_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP NOT NULL,
    ingestion_date DATE NOT NULL,
    is_current BOOLEAN NOT NULL,
    PRIMARY KEY (id, start_date)
)
OPTIONS (
    description='Tags dimension table with SCD type 2'
);
