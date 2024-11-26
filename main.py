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


# Extracción de datos de Sakila
def get_data_from_sakila(query):
    try:
        connection = psycopg2.connect(DATABASE_URI)
        cursor = connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        pprint(results[0:5])
        return results
    except Exception as e:
        print(f"Ocurrió un error en la función get_data_from_sakila: {e}")
        return None
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("Conexión cerrada.")


# Transformación de datos de Sakila
def transform_data():
    try:
        query = """
                    SELECT c.customer_id,
                           INITCAP(c.first_name) AS first_name,
                           INITCAP(c.last_name) AS last_name,
                           c.email,
                           c.create_date::DATE AS just_date,
                           f.title,
                           f.description,
                           f.rating
                    FROM sakila.customer AS c
                    INNER JOIN sakila.rental AS r ON c.customer_id = r.customer_id
                    INNER JOIN sakila.inventory AS i ON r.inventory_id = i.inventory_id
                    INNER JOIN sakila.film AS f ON i.film_id = f.film_id
                """
        data = get_data_from_sakila(query)
        df_db = pd.DataFrame(data, columns=COLUMNS_ORDER_SAKILA)
    except Exception as e:
        print(f"Ocurrió un error en la función transform_data: {e}")
    
    return df_db


# Carga de datos transformados de Sakila a BigQuery
def load_data_to_bq(project_id, if_exists, credentials):
    df_db = transform_data()
    if len(df_db) > 0:
        pd_gbq.to_gbq(
            df_db,
            SAKILA_TABLE_ID,
            project_id,
            if_exists=if_exists,
            credentials=credentials
        )
        print("Datos de Sakila añadidos exitosamente a BigQuery.")
    else:
        print("Nos se pudo actualizar: No hay datos de Sakila para cargar a BigQuery.")


if __name__ == "__main__":
    load_data_to_bq(PROJECT_ID, "replace", CREDENTIALS)
