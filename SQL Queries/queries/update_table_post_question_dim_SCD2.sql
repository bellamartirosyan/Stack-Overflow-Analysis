MERGE {project_id}.{dataset_id}.post_question AS dst
USING (
    SELECT id, 
        post_type_id, 
        owner_user_id, 
        title, 
        body, 
        tags, 
        view_count, 
        answer_count, 
        comment_count, 
        favorite_count, 
        creation_date, 
        last_activity_date, 
        uuid, 
        ingestion_date,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY ingestion_date DESC) AS row_num
    FROM {project_id}.{dataset_id}.staging_post_question
    WHERE id IS NOT NULL
) AS src
ON dst.id = src.id AND dst.is_current = TRUE
WHEN MATCHED AND (
        dst.post_type_id != src.post_type_id OR 
        dst.owner_user_id != src.owner_user_id OR 
        dst.title != src.title OR 
        dst.body != src.body OR 
        dst.tags != src.tags OR 
        dst.view_count != src.view_count OR 
        dst.answer_count != src.answer_count OR 
        dst.comment_count != src.comment_count OR 
        dst.favorite_count != src.favorite_count OR 
        dst.creation_date != src.creation_date OR 
        dst.last_activity_date != src.last_activity_date
    ) THEN
    UPDATE SET dst.is_current = FALSE, dst.effective_end_date = src.ingestion_date
WHEN NOT MATCHED THEN 
    INSERT (id, 
        post_type_id, 
        owner_user_id, 
        title, 
        body, 
        tags, 
        view_count, 
        answer_count, 
        comment_count, 
        favorite_count, 
        creation_date, 
        last_activity_date, 
        uuid, 
        ingestion_date,
        effective_start_date,
        effective_end_date,
        is_current)
    VALUES (
        src.id, 
        src.post_type_id, 
        src.owner_user_id, 
        src.title, 
        src.body, 
        src.tags, 
        src.view_count, 
        src.answer_count, 
        src.comment_count, 
        src.favorite_count, 
        src.creation_date, 
        src.last_activity_date, 
        src.uuid, 
        src.ingestion_date,
        src.ingestion_date,
        CAST('9999-12-31' AS DATE),
        TRUE
    )
WHEN MATCHED AND dst.is_current = FALSE AND src.row_num = 1 THEN
    UPDATE SET dst.is_current = TRUE, dst.effective_start_date = src.ingestion_date;
