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
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
from google_auth_oauthlib.flow import InstalledAppFlow
import time 


# def my_function():
#     logger.basicConfig(format='%(asctime)s - %(message)s', level=logger.INFO)
#     logger.debug('This is a debug message')
#     logger.info('This is an info message')
#     logger.warning('This is a warning message')
#     logger.error('This is an error message')
#     logger.critical('This is a critical message') 

# Authenticate and create a BigQuery client
def create_client(cred_json, project_id):
    credentials = service_account.Credentials.from_service_account_file(cred_json)
    client = bigquery.Client(credentials=credentials, project=project_id)
    return client

# Upload data to Google Drive
def upload_file_from_local_to_drive(gauth_cred, client_config_file,folder_id, filename, file_name):
    gauth = GoogleAuth()

    # Loads previously saved credentials file if available, otherwise authorizes user and saves new credentials.
    gauth.LoadCredentialsFile(gauth_cred)
    if gauth.credentials is None:
        gauth.DEFAULT_SETTINGS['client_config_file'] = client_config_file
        gauth.LocalWebserverAuth()
        gauth.SaveCredentialsFile(gauth_cred)
    drive = GoogleDrive(gauth)
    file=None
    # Uploads the new JSON file to a specified Google Drive folder.
    file = drive.CreateFile({'title': file_name, 'parents': [{'id': folder_id}]})
    file.SetContentFile(filename)
    file.Upload()
  
   
    
def upload_files_from_local_to_drive(gauth_cred,
                                     client_config_file,
                                     folder_id,
                                     download_folder,
                                     dest_file_template,
                                     ingestion_date):
    
    for filename_template in dest_file_template.values():
        print(filename_template)
        destination_file_name = filename_template.format(ingestion_date=ingestion_date.replace("-", ""))
        downloadfolder = os.getcwd() + '/' + download_folder
        dest_file_path = os.path.join(downloadfolder, destination_file_name)
        upload_file_from_local_to_drive(gauth_cred, client_config_file,folder_id=folder_id, filename=dest_file_path, file_name=destination_file_name)
     
#Download data from Bigquery
def retrieve_table(client,project_id,table_name, dataset_id, dest_file_template,ingestion_date,download_folder):

    # Set the name of the source table
    table_id = "{project_id}.{dataset_id}.{table_name}".format(project_id=project_id, dataset_id=dataset_id, table_name=table_name)

    # Set the path to the JSON file to save
    destination_file_name = dest_file_template.format(ingestion_date=ingestion_date.replace("-", ""))
    dest_file_path = os.path.join(download_folder, destination_file_name)
    #"/Users/izabellamartirosyan/Desktop/Capstone/post_answers.json"
    retrieve_data_script = load_query("retrieve_data_{}".format(table_name)).format(
        project_id=project_id, dataset_id=dataset_id, table_name=table_name, ingestion_date=ingestion_date)
    # Execute the query
    query_job = client.query(retrieve_data_script)

    # Save the query results to a JSON file
    with open(dest_file_path, "w") as destination_file:
        query_job.result().to_dataframe().to_json(destination_file, orient="records")

#retrive data
def retrieve_data(client,project_id, table_dict, dataset_id,ingestion_date,download_folder):
    for key,value in table_dict.items():
        retrieve_table(client,project_id,key, dataset_id, value,ingestion_date,download_folder)

 


# Add data from Drive to BigQuery tables
def add_data_to_raw_table(gauth_cred, cred_json, client_config_file, folder_id, project_id, dataset_id, table_name, filename):
    gauth = GoogleAuth()

    # Loads previously saved credentials file if available, otherwise authorizes user and saves new credentials.
    gauth.LoadCredentialsFile(gauth_cred)
    if gauth.credentials is None:
        gauth.DEFAULT_SETTINGS['client_config_file'] = client_config_file
        gauth.LocalWebserverAuth()
        gauth.SaveCredentialsFile(gauth_cred)

    drive = GoogleDrive(gauth)
    

    # Search for the file with the specified name in the specified folder
    query = "title='%s' and '%s' in parents and trashed=false" % (filename, folder_id)
    file_list = drive.ListFile({'q': query}).GetList()

    # Get the ID of the first file that matches the search query
    for file in file_list:
        if file['title'] == filename:
            file_id = file['id']
            break

    if file_id is None:
        print(f"File '{filename}' not found in the folder.")
        return

    # Create a new instance of the GoogleDriveFile class using the file ID
    file = drive.CreateFile({'id': file_id})

    # Download the file content into a pandas dataframe
    local_file_name = file['title']
    file.GetContentFile(local_file_name)
    df = pd.read_json(file['title'])

    df['uuid'] = [uuid.uuid4() for _ in range(len(df.index))]
    df['uuid'] = df['uuid'].astype(str)

    current_date = datetime.now()
    df['ingestion_date'] = current_date.strftime('%Y-%m-%d')
    # upload the DataFrame to the new BigQuery table
    credentials = service_account.Credentials.from_service_account_file(cred_json)
    pandas_gbq.to_gbq(df, f'{project_id}.{dataset_id}.{table_name}', project_id=project_id, credentials=credentials,if_exists='append')
    os.remove(file['title'])


def load_query(query_name):
    query_location = os.getcwd() + config.queries
    for script in os.listdir(query_location):
        if query_name in script:
            with open(query_location + '/' + script, 'r') as script_file:
                sql_script = script_file.read()
            break
    return sql_script


def create_schema(client, project_id, dataset_id):
    create_schema_script = load_query("create_schema_{}".format(dataset_id)).format(
        project_id=project_id, dataset_id=dataset_id)
    client.query(create_schema_script)
    print("The {project_id}.{dataset_id} schema has been created".format(
        project_id=project_id, dataset_id=dataset_id))


def drop_table(client, project_id, dataset_id, table_name):
    drop_table_script = load_query("drop_table").format(
        project_id=project_id, dataset_id=dataset_id, table_name=table_name)
    client.query(drop_table_script)
    print("The {project_id}.{dataset_id}.{table_name} table has been dropped".format(
        project_id=project_id, dataset_id=dataset_id, table_name=table_name))


def create_table(client, project_id, dataset_id, table_name):
    create_table_script = load_query("create_table_{}".format(table_name)).format(
        project_id=project_id, dataset_id=dataset_id, table_name=table_name)
    client.query(create_table_script)
    print("The {project_id}.{dataset_id}.{table_name} table has been created".format(
        project_id=project_id, dataset_id=dataset_id, table_name=table_name))


# Merge temporary tables with the staging raw table
def update_staging_raw(client, project_id, dataset_id, dst_table_name, src_table_name):
    update_table_script = load_query("update_staging_raw.sql").format(
        project_id=project_id,
        dataset_id=dataset_id,
        dst_table_name=dst_table_name,
        src_table_name=src_table_name
    )
    client.query(update_table_script)

    # delete the temporary/source table
    dataset_ref = client.dataset(dataset_id).table(src_table_name)
    client.delete_table(dataset_ref)


def update_dim_date(client, project_id, dataset_id, date_table_name):
    update_table_script = load_query("update_table_{}".format(date_table_name)).format(
        project_id=project_id,
        dataset_id=dataset_id,
        date_table_name=date_table_name
    )
    client.query(update_table_script)

def update_fact_table(client, project_id, dataset_id, dst_table_name, src_table_name):
    update_table_script = load_query("update_table_{}".format(dst_table_name)).format(
        project_id=project_id,
        dataset_id=dataset_id,
        dst_table_name=dst_table_name,
        src_table_name=src_table_name
    )
    client.query(update_table_script)

def update_dim_table(client, project_id, dataset_id, dst_table_name, src_table_name):
    update_table_script = load_query("update_table_{}".format(dst_table_name)).format(
        project_id=project_id,
        dataset_id=dataset_id,
        dst_table_name=dst_table_name,
        src_table_name=src_table_name
    )
    client.query(update_table_script)    

def check_table(client, project_id, dataset_id, table_name):
    check_table_script = load_query("check_table.sql").format(
        project_id=project_id,
        dataset_id=dataset_id,
        table_name=table_name
    )
    job = client.query(check_table_script).to_dataframe()
    if job.empty:
        print('{} table is empty'.format(table_name))
    else:
        print('{} table as dataframe:'.format(table_name))
        print(job)
