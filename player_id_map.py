from google.cloud import bigquery
import requests
import os
import datetime

URL = 'https://www.smartfantasybaseball.com/PLAYERIDMAPCSV'
headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36'}
DS = datetime.datetime.now().strftime("%Y-%m-%d")
FILE_ROOT = "./data/SFBB"
FILE_PATH = f"{FILE_ROOT}/player_id_map_{DS}.csv"

def download_file():
	resp = requests.get(URL, headers=headers)
	if resp.ok:
		if not os.path.exists(FILE_ROOT):
			os.mkdir(FILE_ROOT)
		with open(FILE_PATH, 'w') as outfile:
			outfile.write(str(resp.text))

		return FILE_PATH
	else:
		print(resp.content)
		return None

def upload_to_bigquery(file_path):
    client = bigquery.Client()

    table_id = f"project-baseball.loaders.player_id_map_{DS}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition='WRITE_TRUNCATE',
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

if __name__ == '__main__':
	print('Starting to load player ID map')
	file_path = download_file()
	upload_to_bigquery(file_path)
	print('Finished loading player ID map')
