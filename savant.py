import json
import os
import random
import sys
import time
from sabersql.__main__ import main as sabersql_download
from google.cloud import bigquery
import datetime
import csv

# Retrieve Job-defined env vars
TASK_INDEX = os.getenv("CLOUD_RUN_TASK_INDEX", 0)
TASK_ATTEMPT = os.getenv("CLOUD_RUN_TASK_ATTEMPT", 0)
# # Retrieve User-defined env vars
# SLEEP_MS = os.getenv("SLEEP_MS", 0)
# FAIL_RATE = os.getenv("FAIL_RATE", 0)

DS = datetime.datetime.now().strftime('%Y%m%d')
DATASET_NAME = "baseball_savant_loader"

def download():
    print(f"Starting Task #{TASK_INDEX}, Attempt #{TASK_ATTEMPT}...")

    sabersql_download()

    print(f"completed sabersql download")
    print(f"loading into Cloud Warehouse")



    print(f"Completed Task #{TASK_INDEX}.")

def load_to_cloud(file_path, table_name):
    # Construct a BigQuery client object.
    client = bigquery.Client()
    client.delete_table(f"project-baseball.{DATASET_NAME}.{table_name}", not_found_ok=True)

    table_id = f"project-baseball.{DATASET_NAME}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition='WRITE_APPEND',
    )

    with open(file_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)

    job.result()  # Waits for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )

def combine_csvs(path):
    # combines all CSV files into one to prepare for upload to gcloud
    COMBINED_FILENAME = 'combined.csv'
    combined_path = f"./data/BaseballSavant/2023/{COMBINED_FILENAME}"
    header_saved = False
    with open(combined_path, 'w') as fout:
        for root, dirs, files in os.walk(path):
            for filename in files:
                if filename != f"{COMBINED_FILENAME}":
                    filepath = f"{path}/{filename}"
                    print(f"Loading {filename}...")
                    with open(filepath) as fin:
                        header =  next(fin)
                        if not header_saved:
                            fout.write(header)
                            header_saved = True
                        fout.write("\n".join(fin.readlines())) # write the data
                else:
                    print("skipping output file")

    # return the path to that data eg ./data/BaseballSavant/2023/combined.csv

    return combined_path

def load_savant():
        print("starting up...")
        #download()
        print("loading Savant data to cloud warehouse")
        SAVANT_ROOT = "./data/BaseballSavant/2023"
        savant_data_path = combine_csvs(SAVANT_ROOT)
        print(f"combined data in {savant_data_path}")
        load_to_cloud(
            file_path=savant_data_path,
            table_name=f"savant_{DS}",
            )
        print('loading person data')
        PERSON_FILE = "./data/Person/people.csv"
        load_to_cloud(
            file_path=f"{PERSON_FILE}",
            table_name=f"person_{DS}",
            )

    # except Exception as err:
    #     message = f"Task #{TASK_INDEX}, " \
    #               + f"Attempt #{TASK_ATTEMPT} failed: {str(err)}"

    #     print(json.dumps({"message": message, "severity": "ERROR"}))
    #     sys.exit(1)  # Retry Job Task by exiting the process

if __name__ == '__main__':
    load_savant()