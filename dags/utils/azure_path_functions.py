# import sys
import os
import logging

from dotenv import load_dotenv
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.decorators import task
load_dotenv()

AZURE_STORAGE_ACCOUNT_CONTAINER_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_CONTAINER_NAME", "container_name")
AZURE_STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
AZURE_STORAGE_ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")

