cred_json = "velvety-ring-349218-f9ffa94dc6a5.json"
project_id = "velvety-ring-349218"
# table_name = 'staging_data'
table_name = "staging_data1"
dataset_id = "Stack_Overflow"
folder_id = "1KDmKD807Tlxdr2m-Kls4UFoO0oicO0th"
gauth_cred = "velvety-ring-349218-0b355d98e0e7.json"
queries = "\SQL_Queries\queries"
client_config_file = "client_secrets.json"
log_folder = "logs"
download_folder="downloads"
log_file_flow = "flow.log"
log_file_infr = "infrastructure_initiation.log"
log_file_scrap = "scraping.log"
log_file_prep = "preprocessing.log"
dest_file_templates={
                    "tags": "tags_{ingestion_date}.json",  
                    "users": "users_{ingestion_date}.json",
                    "posts_questions": "posts_questions_{ingestion_date}.json",
                    "post_links": "post_links_{ingestion_date}.json",
                    "post_history": "post_history_{ingestion_date}.json",
                    "comments": "comments_{ingestion_date}.json",
                    "posts_answers": "posts_answers_{ingestion_date}.json"
                    "badges": "badges_{ingestion_date}.json"
                    } 
