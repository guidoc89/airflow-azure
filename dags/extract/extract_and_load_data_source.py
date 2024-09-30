import os
import logging
from airflow.models import DagRun
from airflow.utils.trigger_rule import TriggerRule
import requests
from datetime import datetime, timedelta

from dotenv import load_dotenv
from airflow import DAG
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

from utils.constants import DATA_BASE_URL

load_dotenv()

OWNER = str(os.getenv("OWNER", "airflow"))
START_YEAR = str(os.getenv("START_YEAR", 2000))
END_YEAR = str(os.getenv("END_YEAR", 2024))
# Storage account constants
AZURE_STORAGE_ACCOUNT_CONTAINER_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_CONTAINER_NAME", "container_name")
AZURE_STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME", "account_name")
AZURE_STORAGE_ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY", "account_key")

STORAGE_OPTIONS = {
    "account_key": AZURE_STORAGE_ACCOUNT_KEY,
}
DEFAULT_ARGS = {
    "owner": OWNER,
    "retries": 5,
    "retry_delay": timedelta(minutes=2),
}

def abfs_container_final_path_from_subdirs_filename(subdir: str, blob_name:str) -> str:
    """
    Construct the abfs domain path for a given container in a Gen2 storage account,
        from a given subdirs folder structure and file name.
 
    Example: 
        - If our container "container_name" has the following structure:
            -- container_name/folder1
            -- container_name/folder1/subfolder1/
            -- container_name/folder2/subfolder1/subfolder2/
            -- container_name/folder3
            -- ...

            and we wanted to save a file "data.csv" inside of folder1/subfolder1,
            we would call it using:
                - subdirs="folder1/subfolder1"
                - blob_name="data.csv"

    Parameters
    ----------
    subdir: str

    blob_name : str
        

    Returns
    -------
    str: the final path
        
    """

    return f"abfs://{AZURE_STORAGE_ACCOUNT_CONTAINER_NAME}@{AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/{subdir}/{blob_name}"



@dag(
    schedule=None,
    start_date=datetime(2024, 9, 16),
    catchup=False,
    default_args=DEFAULT_ARGS,
)
def extract_load_data_source_from_github():

    @task(task_id="extract_load_source", retries=2)
    def extract_load_source(dag_run: DagRun | None = None) -> bool:

        """
        This function is going to get called from another DAG via a TriggerDagRunOperator, so make sure that the "dag_run" context is properly passed
            in the "conf" parameter, including the keys "blob_name", "subdir" and "full_csv_path"

        Parameters
        ----------
        dag_run : DagRun | None
            Expected keys inside of "conf": 
                - subdir: str
                - blob_name: str
                - full_csv_path: str
            

        Returns
        -------
        bool
            
        """

        import pandas as pd

        blob_name: str = dag_run.conf.get("blob_name", "blob_name") if dag_run else "blob_name"
        subdir: str = dag_run.conf.get("subdir", "subdir") if dag_run else "subdir"
        full_csv_path: str = dag_run.conf.get("full_csv_path", "full_csv_path") if dag_run else "full_csv_path"

        # TODO: how to gracefully handle wrong file paths or years?
        # Need to show it in the UI as wrong year or wrong file path?
        try:
            df = pd.read_csv(full_csv_path)
            logging.info(f"Starting '{full_csv_path}' data transfer to Azure...")
            df.to_csv(
                abfs_container_final_path_from_subdirs_filename(
                    subdir=subdir,
                    blob_name=blob_name
                ),
                storage_options=STORAGE_OPTIONS,
                index=False,
            )
            logging.info(f"Succesfully inserted file '{blob_name}' into Azure storage account!")
            return True
        except Exception as e:
            raise AirflowFailException(f"Error when trying to write file '{blob_name}' from '{full_csv_path}' to Data Lake Gen2. ERROR: {e}")

    extract_load_source()

extract_load_data_source_from_github()


