

# NOTE: this module is not used, but can be used in order to concanate all the year matches files and import only 1 full file to Azure,
#   instead of generating one task for each CSV data source.


import logging
import os
from datetime import datetime

from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from dotenv import load_dotenv
from airflow.utils.state import TaskInstanceState
from airflow.decorators import dag, task, task_group
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.exceptions import AirflowFailException
from airflow.sensors.external_task import ExternalTaskSensor

from load.check_data_source_need_extract import check_blob_loaded
from extract.extract_and_load_data_source import abfs_container_final_path_from_subdirs_filename
from utils.constants import DATA_BASE_URL


load_dotenv()

OWNER = str(os.getenv("OWNER", "airflow"))
START_YEAR = str(os.getenv("START_YEAR", 2000))
END_YEAR = str(os.getenv("END_YEAR", 2024))
# Storage account constants
AZURE_STORAGE_ACCOUNT_CONTAINER_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_CONTAINER_NAME", "container_name")
AZURE_STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME", "account_name")
AZURE_STORAGE_ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY", "account_key")
AZURE_STORAGE_ACCOUNT_CONTAINER_BRONZE_LAYER = os.getenv("AZURE_STORAGE_ACCOUNT_CONTAINER_BRONZE_LAYER", "bronze")

# Storage account blobs names
# AZURE_MATCHES_BLOB_NAME = os.getenv("AZURE_MATCHES_BLOB_NAME", "matches_blob_name")  # only used if merging the CSVs while reading and uploading only 1 to data lake
AZURE_PLAYERS_BLOB_NAME = os.getenv("AZURE_PLAYERS_BLOB_NAME", "players_blob_name")
AZURE_MATCHES_BLOB_NAME = os.getenv("AZURE_MATCHES_BLOB_NAME", "matches_blob_name")




DEFAULT_ARGS = {
    "owner": OWNER,
}
STORAGE_OPTIONS = {
    "account_key": AZURE_STORAGE_ACCOUNT_KEY,
}


@dag(
    schedule="@daily",
    start_date=datetime(2024, 9, 16),
    catchup=False,
    default_args=DEFAULT_ARGS,
)
def check_all_bronze_data_exists():

    start_all_bronze_data_exists_check = EmptyOperator(task_id="start_all_bronze_data_exists_check")
    end_all_bronze_data_exists_check = EmptyOperator(task_id="end_all_bronze_data_exists_check", trigger_rule=TriggerRule.ALL_SUCCESS)

    azure_hook = WasbHook(wasb_conn_id="azure_blob_conn")

    check_players_bronze_data_exists = check_blob_loaded.override(
        task_id="check_players_bronze_data_exists"
    )(azure_hook, AZURE_STORAGE_ACCOUNT_CONTAINER_BRONZE_LAYER, AZURE_PLAYERS_BLOB_NAME)

    check_matches_bronze_data_exists = check_blob_loaded.override(
        task_id="check_matches_bronze_data_exists"
    )(azure_hook, AZURE_STORAGE_ACCOUNT_CONTAINER_BRONZE_LAYER, AZURE_MATCHES_BLOB_NAME)

    start_all_bronze_data_exists_check >> [check_matches_bronze_data_exists, check_players_bronze_data_exists] >> end_all_bronze_data_exists_check


@dag(
    schedule=None,
    start_date=datetime(2024, 9, 16),
    catchup=False,
    default_args=DEFAULT_ARGS,
)
def get_matches_bronze_data_from_github():
    import pandas as pd

    # start_get_all_bronze_data = EmptyOperator(task_id="start_get_all_bronze_data")
    # end_get_all_bronze_data = EmptyOperator(task_id="end_get_all_bronze_data", trigger_rule=TriggerRule.ALL_SUCCESS)

    @task(task_id=f"get_matches_bronze_{START_YEAR}_{END_YEAR}_data_df_from_github", retries=2)
    def get_matches_bronze_data_df() -> bool:
        # blob_name = f"matches_{str(datetime.now().date())}_{str(datetime.now().time()).replace(':','_')}.csv"
        dfs = []
        for i in range(int(START_YEAR), int(END_YEAR)+1):
            dfs.append(pd.read_csv(f"{DATA_BASE_URL}atp_matches_{i}.csv"))
        df = pd.concat(dfs)

        logging.info(f"Starting matches data transfer to Azure...")
        try:
            df.to_csv(
                abfs_container_final_path_from_subdirs_filename(
                    subdir=AZURE_STORAGE_ACCOUNT_CONTAINER_BRONZE_LAYER,
                    blob_name=AZURE_MATCHES_BLOB_NAME
                ),
                storage_options=STORAGE_OPTIONS,
                index=False,
            )
            logging.info(f"Succesfully inserted file '{AZURE_MATCHES_BLOB_NAME}' into Azure storage account!")
            return True
        except Exception as e:
            raise AirflowFailException(f"Error when trying to write file '{abfs_container_final_path_from_subdirs_filename}' to Data Lake Gen2. ERROR: {e}")

    get_matches_bronze_data_df()

@dag(
    schedule=None,
    start_date=datetime(2024, 9, 16),
    catchup=False,
    default_args=DEFAULT_ARGS,
)
def get_players_bronze_data_from_github():

    @task(task_id="get_players_bronze_data_df_from_github", retries=2)
    def get_players_bronze_data_df() -> bool:
        import pandas as pd
        # blob_name = f"players_{str(datetime.now().date())}_{str(datetime.now().time()).replace(':','_')}.csv"
        df = pd.read_csv(DATA_BASE_URL + "atp_players.csv")

        logging.info(f"Starting players data transfer to Azure...")
        try:
            df.to_csv(
                abfs_container_final_path_from_subdirs_filename(
                    subdir=AZURE_STORAGE_ACCOUNT_CONTAINER_BRONZE_LAYER,
                    blob_name=AZURE_PLAYERS_BLOB_NAME
                ),
                storage_options=STORAGE_OPTIONS,
                index=False,
            )
            logging.info(f"Succesfully inserted file '{AZURE_PLAYERS_BLOB_NAME}' into Azure storage account!")
            return True
        except Exception as e:
            raise AirflowFailException(f"Error when trying to write file '{abfs_container_final_path_from_subdirs_filename}' to Data Lake Gen2. ERROR: {e}")


    get_players_bronze_data_df()



@dag(
    start_date=datetime(2024, 9, 16),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
)
def check_all_bronze_data_ready_pipeline():

    start_all_bronze_data_ready_check = EmptyOperator(task_id="start_all_bronze_data_ready_check")

    @task_group(group_id="players_bronze_data_exists_task_group")
    def players_bronze_data_exists_task_group():

        waiting_for_players_bronze_data_exists_check = ExternalTaskSensor(
            task_id="waiting_for_players_bronze_data_exists_check",
            external_dag_id="check_all_bronze_data_exists",
            external_task_id="check_players_bronze_data_exists",
            allowed_states=[TaskInstanceState.SUCCESS],
            failed_states=[TaskInstanceState.FAILED, TaskInstanceState.SKIPPED],
        )

        trigger_players_bronze_data_download = TriggerDagRunOperator(
            task_id="trigger_players_bronze_data_download",
            trigger_dag_id="get_players_bronze_data_from_github",
            conf=None,
            wait_for_completion=True,
            trigger_rule=TriggerRule.ONE_FAILED,
        )

        @task(trigger_rule=TriggerRule.ALL_DONE)
        def players_bronze_data_ready():
            logging.info("Bronze PLAYERS data is ready")


        (
            waiting_for_players_bronze_data_exists_check
            >> trigger_players_bronze_data_download
            >> players_bronze_data_ready()

        )

    @task_group(group_id="matches_bronze_data_exists_task_group")
    def matches_bronze_data_exists_task_group():

        waiting_for_matches_bronze_data_exists_check = ExternalTaskSensor(
            task_id="waiting_for_matches_bronze_data_exists_check",
            external_dag_id="check_all_bronze_data_exists",
            external_task_id="check_matches_bronze_data_exists",
            allowed_states=[TaskInstanceState.SUCCESS],
            failed_states=[TaskInstanceState.FAILED, TaskInstanceState.SKIPPED],
        )

        trigger_matches_bronze_data_download = TriggerDagRunOperator(
            task_id="trigger_matches_bronze_data_download",
            trigger_dag_id="get_matches_bronze_data_from_github",
            conf=None,
            wait_for_completion=True,
            trigger_rule=TriggerRule.ONE_FAILED,
        )

        @task(trigger_rule=TriggerRule.ALL_DONE)
        def matches_bronze_data_ready():
            logging.info("Bronze MATCH data is ready")

        (
            waiting_for_matches_bronze_data_exists_check
            >> trigger_matches_bronze_data_download
            >> matches_bronze_data_ready()
        )

    trigger_players_data_bronze_to_silver_processing_pipeline = TriggerDagRunOperator(
        task_id="trigger_players_data_bronze_to_silver_processing_pipeline",
        trigger_dag_id="players_data_bronze_to_silver_processing_pipeline",
        conf=None,
        wait_for_completion=True,
    )

    trigger_matches_data_bronze_to_silver_processing_pipeline = TriggerDagRunOperator(
        task_id="trigger_matches_data_bronze_to_silver_processing_pipeline",
        trigger_dag_id="matches_data_bronze_to_silver_processing_pipeline",
        conf=None,
        wait_for_completion=True,
    )

    start_all_bronze_data_ready_check >> players_bronze_data_exists_task_group() >> trigger_players_data_bronze_to_silver_processing_pipeline
    start_all_bronze_data_ready_check >> matches_bronze_data_exists_task_group() >> trigger_matches_data_bronze_to_silver_processing_pipeline
