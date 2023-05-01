CREATE TABLE IF NOT EXISTS {project_id}.{dataset_id}.{table_name}(
    [BadgeID_SK] INT PRIMARY KEY IDENTITY(1,1)
    ,[BadgeID_NK] INT
    ,[UserID] INT
    ,[Name] VARCHAR(50)
    ,[Date] TIMESTAMP
    ,[Class] INT
    ,[TagBased] BOOLEAN
    ,[UUID] STRING
    ,[IngestionDate] DATE
);
