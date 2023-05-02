import os
import config
from datetime import datetime
from google.oauth2 import service_account
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from google.cloud import bigquery
import pandas as pd
import pandas_gbq
import uuid
import argparse

# Authenticate and create a BigQuery client
def create_client(cred_json, project_id):
    credentials = service_account.Credentials.from_service_account_file(cred_json)
    client = bigquery.Client(credentials=credentials, project=project_id)
    return client


def load_query(query_name):
    for script in os.listdir(config.queries):
        if query_name in script:
            with open(config.queries + '\\' + script, 'r') as script_file:
                sql_script = script_file.read()
            break
    return sql_script


def drop_table(client, project_id, dataset_id, table_name):
    drop_table_script = load_query('drop_table').format(project_id=project_id,
                                                        dataset_id=dataset_id,
                                                        table_name=table_name)
    client.query(drop_table_script)
    print("The {project_id}.{dataset_id}.{table_name} table has been dropped".format(project_id=project_id,
                                                                                     dataset_id=dataset_id,
                                                                                     table_name=table_name))


def create_table(client, project_id, dataset_id, table_name):
    create_table_script = load_query('create_table_{}'.format(table_name)).format(project_id=project_id,
                                                                                  dataset_id=dataset_id,
                                                                                  table_name=table_name)
    client.query(create_table_script)
    print("The {project_id}.{dataset_id}.{table_name} table has been created".format(project_id=project_id,
                                                                                     dataset_id=dataset_id,
                                                                                     table_name=table_name))


def create_schema(client, project_id, dataset_id):
    create_schema_script = load_query('create_schema_{}'.format(dataset_id)).format(project_id=project_id,
                                                                                    dataset_id=dataset_id)
    client.query(create_schema_script)
    print("The {project_id}.{dataset_id} schema has been created".format(project_id=project_id, dataset_id=dataset_id))


def update_table(client, project_id, dataset_id, dst_table_name, src_table_name):
    update_table_script = load_query('update_table_{}'.format(dst_table_name)).format(project_id=project_id,
                                                                                      dataset_id=dataset_id,
                                                                                      dst_table_name=dst_table_name,
                                                                                      src_table_name=src_table_name)
    client.query(update_table_script)
    print("The {project_id}.{dataset_id}.{table_name} table has been updated".format(project_id=project_id,
                                                                                     dataset_id=dataset_id,
                                                                                     table_name=dst_table_name))


def upload_from_drive_to_cloud(gauth_cred, client_config_file, project_id, folder_id, dataset_id, table_name):
    gauth = GoogleAuth()
    gauth.LoadCredentialsFile(gauth_cred)

    if gauth.credentials is None:
        # Authenticate and save the credentials in a local file
        gauth.DEFAULT_SETTINGS['client_config_file'] = client_config_file
        gauth.LocalWebserverAuth()
        gauth.SaveCredentialsFile(gauth_cred)

    drive = GoogleDrive(gauth)

    query = f"'{folder_id}' in parents and trashed = false"
    file_list = drive.ListFile({'q': query}).GetList()

    if len(file_list) > 0:
        # Sort file list by modified date
        file_list = sorted(file_list, key=lambda x: x['modifiedDate'], reverse=True)

        # Download the latest modified CSV file
        file_id = file_list[0]['id']
        file = drive.CreateFile({'id': file_id})
        file.GetContentFile(file['title'])

        # Load the downloaded CSV file into a Pandas dataframe
        df = pd.read_csv(file['title'])
        df['Staging_Raw_ID'] = [uuid.uuid4() for _ in range(len(df.index))]
        df['Staging_Raw_ID'] = df['Staging_Raw_ID'].astype(str)

        # Set the project ID and dataset name
        client = bigquery.Client(project=project_id)

        # create a new BigQuery table with an inferred schema
        table_ref = client.dataset(dataset_id).table(table_name)

        job_config = bigquery.LoadJobConfig()
        job_config.autodetect = True
        job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_EMPTY

        client.load_table_from_dataframe(df, table_ref, job_config=job_config).result()

        # upload the DataFrame to the new BigQuery table
        pandas_gbq.to_gbq(df, f'{project_id}.{dataset_id}.{table_name}', project_id=project_id, if_exists='append')

        # pandas_gbq.to_gbq(df, table_name, project_id=project_id, if_exists='fail')
        print(f"Data uploaded.")
    else:
        print("No JSON files found in the specified folder.")


def upload_from_local_to_drive(gauth_cred, client_config_file, original_file_path, folder_id):
    gauth = GoogleAuth()

    # Loads previously saved credentials file if available, otherwise authorizes user and saves new credentials.
    gauth.LoadCredentialsFile(gauth_cred)
    if gauth.credentials is None:
        gauth.DEFAULT_SETTINGS['client_config_file'] = client_config_file
        gauth.LocalWebserverAuth()
        gauth.SaveCredentialsFile(gauth_cred)

    drive = GoogleDrive(gauth)

    # Reads a CSV file from a given path and adds the current date as a column to the dataframe.
    df = pd.read_csv(original_file_path)
    today = datetime.today().strftime('%Y-%m-%d')
    df['ingestion_date'] = today
    df['ingestion_date'] = pd.to_datetime(df['ingestion_date']).dt.date

    # Saves the updated dataframe to a new CSV file.
    file_name = f'{today}_staging_data.csv'
    file_path = '/Users/izabellamartirosyan/Desktop/Capstone/data/{today _staging_data.json'
    
    df.to_csv(file_path, index=False)

    # Uploads the new CSV file to a specified Google Drive folder.
    file = drive.CreateFile({'title': file_name, 'parents': [{'id': folder_id}]})
    file.SetContentFile(file_path)
    file.Upload()
    
    #Argparse module
    import argparse
    parser = argparse. Argumentarser()
    parser.add_argument ("--ingestion-date", help="display a square of a given number",
                        type=str)
    parser .add_argument ("--reload", , help="display a square of a given number",
                        type=str)
    aggs = parser -parse_args()
    print ("The flow is running for", args. ingestion_date, "with reload =", args.reload, ".
)
