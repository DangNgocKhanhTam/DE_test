from google.oauth2 import service_account
from google.cloud import bigquery
import os 
import pandas as pd
import glob 
import json

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

project_id = 'cusma-383203'
dataset_id = 'raw_data'
service_account_key_path = os.path.join(base_dir, "config", "service_account.json")

transaction_file = os.path.join(base_dir,"data", "transactions.csv")
users_file = os.path.join(base_dir,"data", "users.csv")
rates_dir = os.path.join(base_dir, "output", "raw_rates")
def get_bigquery_client(): 
    credentials =  service_account.Credentials.from_service_account_file(service_account_key_path)
    return bigquery.Client(project= project_id, credentials = credentials)

def create_dataset (client):
    dataset = client.dataset(dataset_id)
    try: 
        client.get_dataset(dataset)
        print(f'--------Dataset {dataset_id} already exists---------')
    except Exception:
        dataset = bigquery.Dataset(dataset)
        dataset.location = 'asia-southeast1'
        dataset  = client.create_dataset(dataset, exists_ok = True)
        print(f'-------Created dataset {dataset_id}----------')

def load_csv(file_path, table_name):
    table_id = f'{project_id}.{dataset_id}.{table_name}'
    df = pd.read_csv(file_path)
    if 'created_at' in df.columns:
        df['created_at'] = pd.to_datetime(df['created_at'])
    if "updated_at" in df.columns:
        df['updated_at'] = pd.to_datetime(df['updated_at'])
    job_config = bigquery.LoadJobConfig(
        write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE, 
        source_format = bigquery.SourceFormat.CSV, 
        skip_leading_rows = 1, 
        autodetect = True
    )

    job = client.load_table_from_dataframe(df, table_id, job_config= job_config)
    job.result()
    print(f'-------Loaded {len(df)} rows to {table_id} -----------')


def load_rates (rates_dir):
    table_id = f'{project_id}.{dataset_id}.rates'
    all_rates = []
    for jsonl_file in glob.glob(os.path.join(rates_dir, "*.jsonl")):
        print(f'------Reading {jsonl_file} ... ---------')
        with open(jsonl_file, 'r') as f : 
            for line in f: 
                if line.strip():
                    all_rates.append(json.loads(line))

    if not all_rates:
        print("-------No rates data found---------")
        return 
    
    df = pd.DataFrame(all_rates)
    
    job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    autodetect=True,
    )   

    job = client.load_table_from_dataframe(df, table_id, job_config= job_config)
    job.result()
    print(f'-------Loaded {len(df)} rows to {table_id} -----------')


if __name__ == "__main__":

    # initialize client
    client = get_bigquery_client()
    # create dataset
    create_dataset(client)
    # load transactions
    load_csv(file_path= transaction_file, table_name= 'transaction')
    # load users 
    load_csv(file_path= users_file, table_name= "users")
    # load rates 
    load_rates(rates_dir= rates_dir)



