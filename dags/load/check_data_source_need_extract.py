import os
import logging
from datetime import datetime, timedelta
from airflow.models.dagrun import DagRun

from dotenv import load_dotenv
from airflow import DAG
from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowFailException
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.empty import EmptyOperator


load_dotenv()

OWNER = os.getenv("OWNER", "airflow")
START_YEAR = os.getenv("START_YEAR", 2000)
END_YEAR = os.getenv("END_YEAR", 2024)
# Storage account constants
AZURE_STORAGE_ACCOUNT_CONTAINER_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_CONTAINER_NAME", "container_name")
AZURE_STORAGE_ACCOUNT_CONTAINER_BRONZE_LAYER = os.getenv("AZURE_STORAGE_ACCOUNT_CONTAINER_BRONZE_LAYER", "bronze")
AZURE_BLOB_CONN = os.getenv("AZURE_BLOB_CONN", "azure_blob_conn")

# Storage account blobs names
AZURE_PLAYERS_BLOB_NAME = os.getenv("AZURE_PLAYERS_BLOB_NAME", "players_blob_name")
AZURE_MATCHES_BLOB_NAME = os.getenv("AZURE_MATCHES_BLOB_NAME", "matches_blob_name")

DEFAULT_ARGS = {
    "owner": OWNER,
    # I know for a fact that it doesnt fail, but just in case
    "retries":1,
    "retry_delay":timedelta(seconds=2),
}

@task(retries=3, retry_delay=timedelta(seconds=5))
def check_blob_loaded(wasb_hook: WasbHook, subdir: str, blob_name: str) -> bool:
    """
    Using "wasb_hook" to connect to a Data Storage (already configured in Airflow connections), check if a given blob exists.

    The blob path will get constructed concatenating "subdir" and "blob_name", so we could search for subpaths 
        inside of our container.

    Example: in our "container_name", look for the file "data.csv" inside the subfolder "folder1/subdolfer1.csv",
        (the full path would be "container_name/folder1/subfolder1/data.csv") using:
            - subdir="folder1/subfolder1"
            - blob="data.csv"

    Parameters
    ----------
    wasb_hook : WasbHook
        
    subdir : str
        
    blob_name : str
        
    Returns
    -------
    bool: whether the given blob exists in the input subdir.
        
    """

    blob_exists = wasb_hook.check_for_blob(AZURE_STORAGE_ACCOUNT_CONTAINER_NAME, subdir + "/" + blob_name)
    if blob_exists:
        logging.info(f"Blog '{blob_name}' found in '{subdir}' dir")
        return blob_exists
    else:
        raise AirflowFailException(f"Blog '{blob_name}' NOT found in '{subdir}' dir")


@dag(
    schedule=None,
    start_date=datetime(2024, 9, 16),
    catchup=False,
    default_args=DEFAULT_ARGS,
)
def check_data_source_loaded():

    azure_wasb_hook = WasbHook(wasb_conn_id=AZURE_BLOB_CONN)

    @task(retries=3, retry_delay=timedelta(seconds=5))
    def check_loaded(dag_run: DagRun | None = None) -> bool:
        """
        Using "wasb_hook" to connect to a Data Storage (already configured in Airflow connections), check if a given blob exists.

        This function is going to get caled from another DAG via a TriggerDagRunOperator, so make sure that the "dag_run" context is properly passed
            in the "conf" parameter, including the keys "blob_name" and "subdir"

        The blob path will get constructed concatenating "subdir" and "blob_name", so we could search for subpaths 
            inside of our container.

        Example: in our "container_name", look for the file "data.csv" inside the subfolder "folder1/subdolfer1.csv",
            (the full path would be "container_name/folder1/subfolder1/data.csv") using:
                - subdir="folder1/subfolder1"
                - blob="data.csv"

        Parameters
        ----------
        dag_run : DagRun | None
            Expected keys inside of "dag_run.conf": 
                - subdir: str
                - blob_name: str
            
        Returns
        -------
        bool: whether the given blob exists in the input subdir.
            
        """
        
        blob_name: str = dag_run.conf.get("blob_name", "blob_name") if dag_run else "blob_name"
        subdir: str = dag_run.conf.get("subdir", "subdir") if dag_run else "subdir"

        blob_exists = azure_wasb_hook.check_for_blob(AZURE_STORAGE_ACCOUNT_CONTAINER_NAME, subdir + "/" + blob_name)
        if blob_exists:
            logging.info(f"Blog '{blob_name}' found in '{subdir}' dir")
            return blob_exists
        else:
            raise AirflowFailException(f"Blog '{blob_name}' NOT found in '{subdir}' dir")

    check_loaded()

check_data_source_loaded()

