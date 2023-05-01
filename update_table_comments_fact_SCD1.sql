MERGE {project_id}.{dataset_id}.{table_name} AS dst
USING (
    SELECT 
        id AS CommentID_NK,
        user_id AS UserID,
        post_id AS PostID,
        text AS CommentText,
        score AS Score,
        creation_date AS CreationDate,
        ingestion_date AS IngestionDate,
        MAX(uuid) AS staging_raw_id
    FROM 
        {project_id}.{dataset_id}.{src_table_name}
    WHERE 
        id IS NOT NULL
    GROUP BY 
        id, 
        user_id, 
        post_id, 
        text, 
        score, 
        creation_date, 
        ingestion_date
) AS src
ON dst.CommentID_NK = src.CommentID_NK
WHEN NOT MATCHED THEN
    INSERT (
        CommentID_NK, 
        UserID, 
        PostID, 
        CommentText, 
        Score, 
        CreationDate, 
        IngestionDate, 
        StagingRawID
    ) 
    VALUES (
        src.CommentID_NK, 
        src.UserID, 
        src.PostID, 
        src.CommentText, 
        src.Score, 
        src.CreationDate, 
        src.IngestionDate, 
        src.staging_raw_id
    )
WHEN MATCHED THEN 
    UPDATE SET
        UserID = src.UserID, 
        PostID = src.PostID, 
        CommentText = src.CommentText, 
        Score = src.Score, 
        CreationDate = src.CreationDate, 
        IngestionDate = src.IngestionDate, 
        StagingRawID = src.staging_raw_id;
