
import os
import logging
from datetime import datetime, timedelta

from dotenv import load_dotenv
from airflow import DAG
from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowFailException
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from utils.constants import DATA_BASE_URL

load_dotenv()
OWNER = os.getenv("OWNER", "airflow")
START_YEAR = os.getenv("START_YEAR", "2000")
END_YEAR = os.getenv("END_YEAR", "2024")
# Storage account constants
AZURE_STORAGE_ACCOUNT_CONTAINER_BRONZE_LAYER = os.getenv("AZURE_STORAGE_ACCOUNT_CONTAINER_BRONZE_LAYER", "bronze")

DEFAULT_ARGS = {
    "owner": OWNER,
    "retries":3,
    "retry_delay":timedelta(seconds=5),
}


# Try only some years (for mathces)
MATCHES_CSV_SOURCES_FULL_PATHS = [f"{DATA_BASE_URL}atp_matches_{i}.csv" for i in range(int(START_YEAR), int(END_YEAR))]
PLAYERS_CSV_SOURCES_FULL_PATHS = [f"{DATA_BASE_URL}atp_players.csv"]
ALL_CSV_SOURCES_FULL_PATHS = PLAYERS_CSV_SOURCES_FULL_PATHS + MATCHES_CSV_SOURCES_FULL_PATHS


@dag(
    schedule="@daily",
    start_date=datetime(2024, 9, 16),
    catchup=False,
    default_args=DEFAULT_ARGS,
)
def extract_load_data():

    start_extract_load_data = EmptyOperator(task_id="start_extract_load_data")
    # NOTE: all done is right? one success and let the user see the tasks that have failed and why? 
    end_extract_load_data = EmptyOperator(task_id="end_extract_load_data", trigger_rule=TriggerRule.ALL_DONE)

    @task_group(group_id="data_sources")
    def extract_load_data_sources():
        
        # MATCHES SOURCES 
        for FULL_CSV_PATH in ALL_CSV_SOURCES_FULL_PATHS:

            # The full GitHub path has characters that can't be used as taks IDs, so just take the last part of it
            CSV_SOURCE = FULL_CSV_PATH.split("/")[-1]
            TASK_NAME = CSV_SOURCE.split(".")[0]  # dots cant be used as IDs, so take the name before it 

            @task_group(group_id=f"load_source_{TASK_NAME}")
            def extract_load_current_data_source():

                trigger_loaded_check = TriggerDagRunOperator(
                    task_id=f"trigger_check_source_{TASK_NAME}_loaded",
                    trigger_dag_id="check_data_source_loaded",
                    conf={
                        "subdir":AZURE_STORAGE_ACCOUNT_CONTAINER_BRONZE_LAYER,
                        "blob_name":CSV_SOURCE,
                    },
                    wait_for_completion=True,
                    trigger_rule=TriggerRule.ALL_SUCCESS,
                )

                trigger_extract = TriggerDagRunOperator(
                    task_id=f"trigger_extract_load_source_{TASK_NAME}",
                    trigger_dag_id="extract_load_data_source_from_github",
                    conf={
                        "subdir":AZURE_STORAGE_ACCOUNT_CONTAINER_BRONZE_LAYER,
                        "blob_name":CSV_SOURCE,
                        "full_csv_path":FULL_CSV_PATH,
                    },
                    wait_for_completion=True,
                    trigger_rule=TriggerRule.ONE_FAILED,
                )

                @task(task_id=f"{TASK_NAME}_loaded", trigger_rule=TriggerRule.NONE_FAILED)
                def source_loaded():
                    logging.info(f"Source '{CSV_SOURCE}' is ready for bronze-silver!")

                # Per-source processing 
                (
                    trigger_loaded_check 
                    >> trigger_extract 
                    >> source_loaded()
                )
                
            extract_load_current_data_source() 

    trigger_transform_bronze_to_silver = TriggerDagRunOperator(
        task_id="trigger_transform_bronze_to_silver",
        trigger_dag_id="transform_bronze_to_silver",
        wait_for_completion=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    trigger_transform_silver_to_gold = TriggerDagRunOperator(
        task_id="trigger_transform_silver_to_gold",
        trigger_dag_id="transform_silver_to_gold",
        wait_for_completion=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    end_transform_bronze_to_silver = EmptyOperator(task_id="end_transform_bronze_to_silver", trigger_rule=TriggerRule.ALL_SUCCESS)
    end_transform_silver_to_gold = EmptyOperator(task_id="end_transform_silver_to_gold", trigger_rule=TriggerRule.ALL_SUCCESS)

    # Global ordering
    # TODO: maybe create a Pipeline script, since otherwise I'm going to dump everything into the load data DAG, when in reality I'm extracting, 
    #   loading, transforming, etc. So maybe better to just 
    (
        start_extract_load_data
        >> extract_load_data_sources() 
        >> end_extract_load_data
        >> trigger_transform_bronze_to_silver
        >> end_transform_bronze_to_silver
        >> trigger_transform_silver_to_gold
        >> end_transform_silver_to_gold

    )


extract_load_data()
