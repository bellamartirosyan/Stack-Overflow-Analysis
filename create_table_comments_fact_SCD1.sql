CREATE TABLE IF NOT EXISTS {project_id}.{dataset_id}.{table_name}(
    [CommentID_SK] INT PRIMARY KEY IDENTITY(1,1)
    ,[CommentID_NK] INT
    ,[UserID] INT
    ,[PostID] INT
    ,[CommentText] VARCHAR(1000)
    ,[Score] INT
    ,[CreationDate] TIMESTAMP
    ,[IngestionDate] DATE
);