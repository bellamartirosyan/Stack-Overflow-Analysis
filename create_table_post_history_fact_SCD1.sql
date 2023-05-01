CREATE TABLE IF NOT EXISTS {project_id}.{dataset_id}.{table_name}(
    [id_SK] INT PRIMARY KEY IDENTITY(1,1),
    [id_NK] INT,
    [post_id] INT,
    [user_id] INT,
    [revision_guid] STRING,
    [creation_date] TIMESTAMP,
    [text] STRING,
    [comment] STRING,
    [uuid] STRING,
    [ingestion_date] DATE
);