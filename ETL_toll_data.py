from airflow.providers.standard.operators.python import PythonOperator
from pathlib import Path
import pandas as pd
from airflow import DAG
from datetime import datetime, timedelta
import tarfile
import csv
import requests

# Define the variables
BASE_DIR = Path.cwd()
URL = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz"
input_file = "tolldata.tgz"
extracted_file = "extracted_data.csv"
transformed_file = "transformed_data.csv"

column_names = [
    "Rowid",
    "Timestamp",
    "Anonymized_Vehicle_number",
    "Vehicle_type",
    "Number_of_axles",
    "Tollplaza_id",
    "Tollplaza_code",
    "Type_of_Payment code",
    "Vehicle_Code",
]

# Create folders for data
folder_raw_data = BASE_DIR / "raw_data"
folder_raw_data.mkdir(exist_ok=True)
folder_extracted_data = BASE_DIR / "extracted_data"
folder_extracted_data.mkdir(exist_ok=True)
folder_transfomed_data = BASE_DIR / "transformed_data"
folder_transfomed_data.mkdir(exist_ok=True)


def download_dataset(url=URL):
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(input_file, "wb") as f:
            f.write(response.raw.read())
        print("File download successefuly")
    else:
        print("Failed download the file")


def untar_dataset():
    with tarfile.open(input_file, "r:gz") as tar:
        tar.extractall(folder_raw_data)
        Path(input_file).unlink()
    print("File dearchiving successefuly")


def extract_data_from_csv():
    with open(f"{folder_raw_data}/vehicle-data.csv", "r") as infile, open(
        f"{folder_extracted_data}/csv_data.csv", "w") as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        for line in reader:
            if len(line) >= 4:
                Rowid = line[0]
                Timestamp = line[1]
                Anonymized_Vehicule_number = line[2]
                Vehicule_type = line[3]
                writer.writerow(
                    [Rowid, Timestamp, Anonymized_Vehicule_number, Vehicule_type]
                )
    print("File extraction CSV successefuly")


def extract_data_from_tsv():
    with open(f"{folder_raw_data}/tollplaza-data.tsv", "r") as infile, open(
        f"{folder_extracted_data}/tsv_data.csv", "w") as outfile:
        reader = csv.reader(infile, delimiter="\t")
        writer = csv.writer(outfile)

        for line in reader:
            if len(line) >= 6:
                n_axles = line[4]
                tollplaza_id = line[5]
                tollplaza_code = line[6]
                writer.writerow([n_axles, tollplaza_id, tollplaza_code])
    print("File extraction TSV successefuly")


def extract_data_from_fixed_widthto():
    with open(f"{folder_raw_data}/payment-data.txt", "r") as infile, open(
        f"{folder_extracted_data}/fixed_width_data.csv", "w") as outfile:

        writer = csv.writer(outfile)

        for line in infile:
            type_payment_code = line[58:61]
            vehicule_code = line[62:67]
            writer.writerow([type_payment_code, vehicule_code])
    print("File extraction TXT successefuly")


def consolidate_data():
    df_csv = pd.read_csv(folder_extracted_data / "csv_data.csv", header=None)
    df_tsv = pd.read_csv(folder_extracted_data / "tsv_data.csv", header=None)
    df_txt = pd.read_csv(folder_extracted_data / "fixed_width_data.csv", header=None)
    df = pd.concat([df_csv, df_tsv, df_txt], axis=1)
    df.columns = column_names
    df.to_csv(folder_extracted_data / extracted_file, index=False)
    print("File consolidation successfulty")


def transform_data():
    df_extracted_data = pd.read_csv(folder_extracted_data / extracted_file)
    df_extracted_data["Vehicle_type"] = df_extracted_data["Vehicle_type"].apply(lambda x: x.upper())
    df_extracted_data.to_csv(folder_transfomed_data / transformed_file)


# DAG arguments
default_args = {
    "owner": "etl_toll_data",
    "start_date": datetime(2025, 10, 10),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# Define the DAG
dag = DAG(
    "etl_toll_data",
    default_args=default_args,
    description="Trafic data pipeline",
    schedule="* * * * *",
    catchup=False
)

# Define tasks
download_task = PythonOperator(task_id="download_dataset", python_callable=download_dataset, dag=dag)

untar_task = PythonOperator(task_id="undar_dataset", python_callable=untar_dataset, dag=dag)

extract_csv_task = PythonOperator(task_id="extract_data_from_csv", python_callable=extract_data_from_csv, dag=dag)

extract_tsv_task = PythonOperator(task_id="extract_data_from_tsv", python_callable=extract_data_from_tsv, dag=dag)

extract_txt_task = PythonOperator(task_id="extract_data_from_txt", python_callable=extract_data_from_fixed_widthto, dag=dag)

consolidate_task = PythonOperator(task_id="consolidate_data", python_callable=consolidate_data, dag=dag)

transform_task = PythonOperator(task_id="transform_data", python_callable=transform_data, dag=dag)

# Define Pipeline
download_task >> untar_task >> [extract_csv_task, extract_tsv_task, extract_txt_task] >> consolidate_task >> transform_task