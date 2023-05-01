CREATE TABLE IF NOT EXISTS {project_id}.{dataset_id}.post_question (
    id INT64,
    post_type_id INT64,
    owner_user_id INT64,
    title STRING,
    body STRING,
    tags STRING,
    view_count INT64,
    answer_count INT64,
    comment_count INT64,
    favorite_count INT64,
    creation_date TIMESTAMP,
    last_activity_date TIMESTAMP,
    uuid STRING,
    ingestion_date DATE,
    effective_start_date DATE,
    effective_end_date DATE,
    is_current BOOLEAN
);
