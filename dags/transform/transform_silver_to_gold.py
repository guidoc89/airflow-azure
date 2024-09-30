
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
AZURE_DATABRICKS_SILVER_TO_GOLD_ID = os.getenv("AZURE_DATABRICKS_SILVER_TO_GOLD_ID", "azure_databricks_silver_to_gold_job_id")

DEFAULT_ARGS = {
    "owner": OWNER
}

@dag(
    default_args=DEFAULT_ARGS,
    schedule=None,
)
def transform_silver_to_gold():

    start_transform_silver_to_gold = EmptyOperator(task_id="start_transform_silver_to_gold")
    end_transform_silver_to_gold = EmptyOperator(task_id="end_transform_silver_to_gold", trigger_rule=TriggerRule.ALL_SUCCESS)

    transform_notebook = DatabricksRunNowOperator(
        task_id="transform_silver_to_gold_notebook",
        # airflow UI created connection
        databricks_conn_id=AZURE_DATABRICKS_CONN,  
        # job ID of the notebook
        job_id=str(AZURE_DATABRICKS_SILVER_TO_GOLD_ID),  
    )
    
    (
        start_transform_silver_to_gold
        >> transform_notebook
        >> end_transform_silver_to_gold
    )



transform_silver_to_gold()
