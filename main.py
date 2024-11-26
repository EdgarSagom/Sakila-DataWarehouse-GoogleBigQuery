# Dependencias
import pandas as pd
import pandas_gbq as pd_gbq
import psycopg2
from google.oauth2 import service_account
from dotenv import load_dotenv
import os
from pprint import pprint

load_dotenv()

# Cadena de conexión, esquemas y demás configuraciones (Setup)
DATABASE_URI = os.getenv("DATABASE_URI")
CREDENTIALS = service_account.Credentials.from_service_account_file(
    "key.json",
)
PROJECT_ID = os.getenv("PROJECT_ID")
SAKILA_TABLE_ID = os.getenv("SAKILA_TABLE_ID")

SAKILA_SCHEMA = [
    {"name": "customer_id", "type": "INT64", "mode": "REQUIRED"},
    {"name": "first_name", "type": "STRING", "mode": "REQUIRED"},
    {"name": "last_name", "type": "STRING", "mode": "REQUIRED"},
    {"name": "email", "type": "STRING", "mode": "NULLABLE"},
    {"name": "just_date", "type": "DATE", "mode": "REQUIRED"},
    {"name": "title", "type": "STRING", "mode": "REQUIRED"},
    {"name": "description", "type": "STRING", "mode": "NULLABLE"},
    {"name": "rating", "type": "STRING", "mode": "NULLABLE"},
]
COLUMNS_ORDER_SAKILA = [col["name"] for col in SAKILA_SCHEMA]

