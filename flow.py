import config
import tasks
from datetime import datetime
import os
import argparse
from logger_flow import *

if __name__ == '__main__':
    client = tasks.create_client(cred_json=config.cred_json, project_id=config.project_id)

    tasks.update_table(client=client, project_id=config.project_id, dataset_id=config.dataset_id,
                       dst_table_name='users_dim', src_table_name=config.table_name)
    tasks.update_table(client=client, project_id=config.project_id, dataset_id=config.dataset_id,
                       dst_table_name='tags_dim', src_table_name=config.table_name)
    tasks.update_table(client=client, project_id=config.project_id, dataset_id=config.dataset_id,
                       dst_table_name='posts_question_dim', src_table_name=config.table_name)
    tasks.update_table(client=client, project_id=config.project_id, dataset_id=config.dataset_id,
                       dst_table_name='badges_fact', src_table_name=config.table_name)
    tasks.update_table(client=client, project_id=config.project_id, dataset_id=config.dataset_id,
                       dst_table_name='comments_fact', src_table_name=config.table_name)
    tasks.update_table(client=client, project_id=config.project_id, dataset_id=config.dataset_id,
                       dst_table_name='post_answers_fact', src_table_name=config.table_name)
    tasks.update_table(client=client, project_id=config.project_id, dataset_id=config.dataset_id,
                       dst_table_name='post_history_fact', src_table_name=config.table_name)
    tasks.update_table(client=client, project_id=config.project_id, dataset_id=config.dataset_id,
                       dst_table_name='post_links_fact', src_table_name=config.table_name)



parser = argparse.ArgumentParser(description="Script description")
parser.add_argument(
    "--ingestion_date", type=str, required=True, help="The ingestion date of this data"
)
parser.add_argument(
    "--reload",
    default=None,
    type=bool,
    required=False,
    help="Module to reload",
)
args = parser.parse_args()

if args.ingestion_date:
    print(f"The ingestion date is {args.ingestion_date}")

if __name__ == "__main__":
    client = tasks.create_client(
        cred_json=config.cred_json, project_id=config.project_id
    )
    logging.info(f"Client has been created in the following location {config.project_id}")

    # Upload csv file from local computer to Google Drive folder
    tasks.upload_from_local_to_drive(
        gauth_cred=config.gauth_cred,
        client_config_file=config.client_config_file,
        original_file_path=config.original_file_path,
        folder_id=config.folder_id,
    )
    logging.info(
        f"Data has been uploaded into the following location {config.folder_id} folder of Google Drive"
    )

    if args.reload:
        tasks.delete_from_table(
            client=client,
            project_id=config.project_id,
            dataset_id=config.dataset_id,
            table_name=config.table_name,
            ingestion_date=args.ingestion_date,
        )

    # Upload csv file from Google Drive to Google Cloud project
    tasks.ingest_from_archive_to_staging_raw(
        gauth_cred=config.gauth_cred,
        project_id=config.project_id,
        table_name=config.table_name,
        dataset_id=config.dataset_id,
        folder_id=config.folder_id,
        client_config_file=config.client_config_file,
    )

    logging.info(
        f"Data has been uploaded into Google Drive to the following location {config.folder_id}, {config.dataset_id} dataset with the "
        f"name {config.table_name}."
    )

    # Updating tables in BigQuery
    tasks.update_dim_table(
        client=client,
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        dst_table_name="DimDistrict_SCD1",
        src_table_name=config.table_name,
        ingestion_date=args.ingestion_date,
    )
    logging.info(f"Data has been uploaded into badges_fact.")
    tasks.update_dim_table(
        client=client,
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        dst_table_name="badges_fact",
        src_table_name=config.table_name,
        ingestion_date=args.ingestion_date,
    )
    logging.info(f"Data has been uploaded into comments_fact.")
    tasks.update_dim_table(
        client=client,
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        dst_table_name="comments_fact",
        src_table_name=config.table_name,
        ingestion_date=args.ingestion_date,
    )
    logging.info(f"Data has been uploaded into post_answers_fact.")
    tasks.update_dim_table(
        client=client,
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        dst_table_name="post_answers_fact",
        src_table_name=config.table_name,
        ingestion_date=args.ingestion_date,
    )
    logging.info(f"Data has been uploaded into post_history_fact.")
    tasks.update_dim_table(
        client=client,
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        dst_table_name="post_history_fact",
        src_table_name=config.table_name,
        ingestion_date=args.ingestion_date,
    )
    logging.info(f"Data has been uploaded into post_links_fact.")
    tasks.update_dim_table(
        client=client,
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        dst_table_name="post_links_fact",
        src_table_name=config.table_name,
        ingestion_date=args.ingestion_date,
    )
    logging.info(f"Data has been uploaded into posts_question_dim.")
    tasks.update_dim_table(
        client=client,
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        dst_table_name="posts_question_dim",
        src_table_name=config.table_name,
        ingestion_date=args.ingestion_date,
    )
    logging.info(f"Data has been uploaded into tags_dim.")
    tasks.update_dim_table(
        client=client,
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        dst_table_name="tags_dim",
        src_table_name=config.table_name,
        ingestion_date=args.ingestion_date,
    )
    logging.info(f"Data has been uploaded into users_dim.")
    tasks.update_fact_table(
        client=client,
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        dst_table_name="users_dim",
        src_table_name=config.table_name,
        ingestion_date=args.ingestion_date,
    )
    logging.info(f"Data has been uploaded into Users Dim.")
