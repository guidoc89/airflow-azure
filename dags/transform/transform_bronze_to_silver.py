
import os
from datetime import datetime

from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from dotenv import load_dotenv
from airflow.decorators import dag, task
from airflow.providers.databricks.operators.databricks import DatabricksWorkflowTaskGroup, DatabricksNotebookOperator, DatabricksRunNowOperator


load_dotenv()
OWNER = os.getenv("OWNER", "airflow")
AZURE_DATABRICKS_CONN = os.getenv("AZURE_DATABRICKS_CONN", "azure_databricks_conn")
AZURE_DATABRICKS_BRONZE_TO_SILVER_JOB_ID = os.getenv("AZURE_DATABRICKS_BRONZE_TO_SILVER_JOB_ID", "azure_databricks_bronze_to_silver_job_id")

DEFAULT_ARGS = {
    "owner": OWNER
}

@dag(
    default_args=DEFAULT_ARGS,
    schedule=None,
)
def transform_bronze_to_silver():

    start_transform_bronze_to_silver = EmptyOperator(task_id="start_transform_bronze_to_silver")
    end_transform_bronze_to_silver = EmptyOperator(task_id="end_transform_bronze_to_silver", trigger_rule=TriggerRule.ALL_SUCCESS)

    transform_notebook = DatabricksRunNowOperator(
        task_id="transform_bronze_to_silver_notebook",
        databricks_conn_id=AZURE_DATABRICKS_CONN,  # airflow UI created connection
        job_id=str(AZURE_DATABRICKS_BRONZE_TO_SILVER_JOB_ID),  # job ID of the notebook
    )
    
    (
        start_transform_bronze_to_silver
        >> transform_notebook
        >> end_transform_bronze_to_silver
    )



transform_bronze_to_silver()
