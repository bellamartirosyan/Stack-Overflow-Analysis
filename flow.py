import config
import tasks
from datetime import datetime
import os
import argparse
from logger_flow import *
import time

# if __name__ == '__main__':
#     client = tasks.create_client(cred_json=config.cred_json, project_id=config.project_id)
#
#     tasks.update_table(client=client, project_id=config.project_id, dataset_id=config.dataset_id,
#                        dst_table_name='users_dim', src_table_name=config.table_name)
#     tasks.update_table(client=client, project_id=config.project_id, dataset_id=config.dataset_id,
#                        dst_table_name='tags_dim', src_table_name=config.table_name)
#     tasks.update_table(client=client, project_id=config.project_id, dataset_id=config.dataset_id,
#                        dst_table_name='posts_question_dim', src_table_name=config.table_name)
#     tasks.update_table(client=client, project_id=config.project_id, dataset_id=config.dataset_id,
#                        dst_table_name='badges_fact', src_table_name=config.table_name)
#     tasks.update_table(client=client, project_id=config.project_id, dataset_id=config.dataset_id,
#                        dst_table_name='comments_fact', src_table_name=config.table_name)
#     tasks.update_table(client=client, project_id=config.project_id, dataset_id=config.dataset_id,
#                        dst_table_name='post_answers_fact', src_table_name=config.table_name)
#     tasks.update_table(client=client, project_id=config.project_id, dataset_id=config.dataset_id,
#                        dst_table_name='post_history_fact', src_table_name=config.table_name)
#     tasks.update_table(client=client, project_id=config.project_id, dataset_id=config.dataset_id,
#                        dst_table_name='post_links_fact', src_table_name=config.table_name)
#

parser = argparse.ArgumentParser(description="My script description")
parser.add_argument(
    "--ingestion_date", type=str, required=True, help="The ingestion date for the data"
)
parser.add_argument(
    "--reload",
    default=None,
    type=bool,
    required=False,
    help="Whether to reload the module",
)
args = parser.parse_args()

if args.ingestion_date:
    print(f"The ingestion date is {args.ingestion_date}")

if __name__ == "__main__":
    client = tasks.create_client(
        cred_json=config.cred_json, project_id=config.project_id
    )
    logging.info(f"Client has been created in the following location {config.project_id}")

    tasks.retrieve_data(
        client=client, project_id=config.project_id, table_dict=config.dest_file_templates, dataset_id=config.dataset_id,ingestion_date=str(args.ingestion_date),download_folder=config.download_folder

    )
    tasks.upload_files_from_local_to_drive(gauth_cred=config.gauth_cred,
                                     client_config_file=config.client_config_file,
                                     folder_id=config.folder_id,
                                     download_folder=config.download_folder,
                                     dest_file_template=config.dest_file_templates,
                                     ingestion_date=args.ingestion_date)
    
    tasks.add_data_to_raw_table(gauth_cred=config.gauth_cred,
                                    cred_json=config.cred_json,
                                    client_config_file=config.client_config_file,
                                    folder_id=config.folder_id,
                                    project_id=config.project_id, dataset_id=config.dataset_id,
                                    table_name=config.table_name
    )
    time.sleep(30)
    tasks.update_dim_table(
                            client=client,
                            project_id=config.project_id, 
                           dataset_id=config.dataset_id,
                           dst_table_name='dim_tags',
                           ingestion_date=args.ingestion_date,
                           src_table_name='staging_raw_tags'
                           )
    logging.info('Downloaded file has been removed')
    if args.reload:
        tasks.delete_from_table(
            #client=client,
            project_id=config.project_id,
            dataset_id=config.dataset_id,
            table_name=config.table_name,
            ingestion_date=args.ingestion_date,
        )

     # Upload json file from Google Drive to Google Cloud project
    tasks.ingest_from_archive_to_staging_raw(
        client=client,
        cred_json=config.cred_json,
        gauth_cred=config.gauth_cred,
        project_id=config.project_id,
        table_name=config.table_name,
        dataset_id=config.dataset_id,
        folder_id=config.folder_id,
        client_config_file=config.client_config_file,
        
    )

    logging.info(
        f"Data has been uploaded into Google Cloud {config.folder_id}, {config.dataset_id} dataset with the "
        f"name {config.table_name}."
    )

    # Updating tables in BigQuery
    logging.info(f"Data has been uploaded into badges_fact.")
    tasks.update_dim_table(
        #client=client,
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        dst_table_name="badges_fact",
        src_table_name=config.table_name,
        ingestion_date=args.ingestion_date,
    )
    logging.info(f"Data has been uploaded into comments_fact.")
    tasks.update_dim_table(
        #client=client,
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        dst_table_name="comments_fact",
        src_table_name=config.table_name,
        ingestion_date=args.ingestion_date,
    )
    logging.info(f"Data has been uploaded into post_answers_fact.")
    tasks.update_dim_table(
        #client=client,
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        dst_table_name="post_answers_fact",
        src_table_name=config.table_name,
        ingestion_date=args.ingestion_date,
    )
    logging.info(f"Data has been uploaded into post_history_fact.")
    tasks.update_dim_table(
        #client=client,
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        dst_table_name="post_history_fact",
        src_table_name=config.table_name,
        ingestion_date=args.ingestion_date,
    )
    logging.info(f"Data has been uploaded into post_links_fact.")
    tasks.update_dim_table(
        #client=client,
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        dst_table_name="post_links_fact",
        src_table_name=config.table_name,
        ingestion_date=args.ingestion_date,
    )
    logging.info(f"Data has been uploaded into posts_question_dim.")
    tasks.update_dim_table(
        #client=client,
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        dst_table_name="posts_question_dim",
        src_table_name=config.table_name,
        ingestion_date=args.ingestion_date,
    )
    logging.info(f"Data has been uploaded into tags_dim.")
    tasks.update_dim_table(
        #client=client,
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        dst_table_name="tags_dim",
        src_table_name=config.table_name,
        ingestion_date=args.ingestion_date,
    )
    logging.info(f"Data has been uploaded into users_dim.")
    tasks.update_fact_table(
        #client=client,
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        dst_table_name="users_dim",
        src_table_name=config.table_name,
        ingestion_date=args.ingestion_date,
    )
    logging.info(f"Data has been uploaded into Users Dim.")
