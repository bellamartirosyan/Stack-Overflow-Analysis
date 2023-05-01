MERGE {project_id}.{dataset_id}.{dst_table_name} dst
using (SELECT userid,
              name,
              date,
              class,
              tag_based,
              uuid,
              ingestion_date,
              Max(staging_raw_id) AS staging_raw_id
       FROM   {project_id}.{dataset_id}.{src_table_name}
       WHERE userid IS NOT NULL
       AND name IS NOT NULL
       AND date IS NOT NULL
       AND class IS NOT NULL
       AND tag_based IS NOT NULL
       AND uuid IS NOT NULL
       GROUP  BY userid,
                 name,
                 date,
                 class,
                 tag_based,
                 uuid,
                 ingestion_date
                 ) src
ON dst.badgeid_nk = src.userid
AND dst.name = src.name
AND dst.date = src.date
AND dst.class = src.class
AND dst.tagbased = src.tag_based
WHEN NOT matched THEN
  INSERT (badgeid_sk,
          badgeid_nk,
          userid,
          name,
          date,
          class,
          tagbased,
          uuid,
          ingestiondate,
          start_date,
          end_date,
          current_flag,
          staging_raw_id)
  VALUES(Generate_uuid(),
         src.userid,
         src.userid,
         src.name,
         src.date,
         src.class,
         src.tag_based,
         src.uuid,
         src.ingestion_date,
         src.ingestion_date,
         DATE('9999-12-31'),
         true,
         src.staging_raw_id)
WHEN matched AND (dst.name <> src.name
                   OR dst.class <> src.class
                   OR dst.tagbased <> src.tag_based
                   ) THEN
  UPDATE SET end_date = src.ingestion_date,
             current_flag = false
  INSERT (badgeid_sk,
          badgeid_nk,
          userid,
          name,
          date,
          class,
          tagbased,
          uuid,
          ingestiondate,
          start_date,
          end_date,
          current_flag,
          staging_raw_id)
  VALUES(Generate_uuid(),
         src.userid,
         src.userid,
         src.name,
         src.date,
         src.class,
         src.tag_based,
         src.uuid,
         src.ingestion_date,
         src.ingestion_date,
         DATE('9999-12-31'),
         true,
         src.staging_raw_id);
