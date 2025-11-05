# Prueba Ingenieria de datos Pragma
# Candidato: Julian Romero
# Importar librerias que se usaran en el Pipeline

from airflow import DAG 
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException
from airflow.sdk import Variable
from psycopg2.extras import execute_values
from datetime import datetime
import pandas as pd
import numpy as np
import os
import logging

# Configuracion de varables

POSTGRES_CONN_ID = "postgres_local" #id de la coneccion DB postgreSQL
TABLE_NAME = "transaction_table" #Nombre de la tabla para archivos 2012-#.csv
VALIDATION_TABLE_NAME = "validation_table" #Nombre de la tabla para validation.csv
TMP_DIR = "/tmp"  # carpeta temporal para pasar datos entre tareas
CLEAN_CSV_NAME = "clean_transaction.csv" # Nombre de archivo para XCom
VALIDATION_CLEAN_CSV_NAME = "clean_validation.csv" # Nombre de archivo para XCom

# Variable para registrar los logs del pipeline
logger = logging.getLogger(__name__)
logger.setLevel("INFO")

# Leer, procesar y guarda un CSV temporal. Retorna la RUTA (XCom) para archivos 2012-#.csv
def read_data():
    # crear una variable global en airflow para automatizar la lectura de los archivos archivos 2012-#.csv
    global COUNT_FILES 
    COUNT_FILES = int(Variable.get("COUNT_FILES", default="1"))
    RAW_DATA_PATH = f"/Users/julianromero/Downloads/dataPruebaDataEngineer/2012-{COUNT_FILES}.csv"
    
    # Exceptiones para evitar errores cuando ya no detecte un archivo
    if not os.path.isfile(RAW_DATA_PATH):
        # Registro del evento
        logger.info(f"###Ya no hay más archivos para procesar. No se encontró: {RAW_DATA_PATH}")
        raise AirflowSkipException(f"No existe el archivo: {RAW_DATA_PATH}")

    # Procesamiento de archivos 2012-#.csv almacenandolos en un df
    df = pd.read_csv(RAW_DATA_PATH)
    # Registro del evento
    logger.info("###Procesando %s con %d registros.", RAW_DATA_PATH, len(df))
    Variable.set("COUNT_FILES", str(COUNT_FILES + 1))# Incremento en la variable COUNT_FILES, para procesar el siguiente archivo

    # Ajuste del tipo de dato, tipo de dato compatible para almacenarlo en la DB
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["price"]     = pd.to_numeric(df["price"], errors="coerce")
    df["user_id"]   = pd.to_numeric(df["user_id"], errors="coerce")

    # Guardar y cargar df(csv) a  Xcom (Airflow)
    os.makedirs(TMP_DIR, exist_ok=True)
    out_path = os.path.join(TMP_DIR, CLEAN_CSV_NAME)
    df.to_csv(out_path, index=False)

    # Registro del evento
    logger.info("###DataFrame leído y guardado en %s con %d filas.", out_path, len(df))
    
    return out_path  # Almacenamiento del archivo al path en XCom

# Leer, procesar y guarda un CSV temporal. Retorna la RUTA (XCom) para archivos validation.csv
def read_validation_data():
    
    VALIDATION_RAW_DATA_PATH = f"/Users/julianromero/Downloads/dataPruebaDataEngineer/validation.csv"

    # Procesamiento de archivos validation.csv almacenandolos en un df
    df = pd.read_csv(VALIDATION_RAW_DATA_PATH)
    # Registro del evento
    logger.info("###Procesando %s con %d registros.", VALIDATION_RAW_DATA_PATH, len(df))

    # Limpieza / tipado robusto
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["price"]     = pd.to_numeric(df["price"], errors="coerce")
    df["user_id"]   = pd.to_numeric(df["user_id"], errors="coerce")

    # Guardar y cargar df(csv) a  Xcom (Airflow)
    os.makedirs(TMP_DIR, exist_ok=True)
    out_path_validation = os.path.join(TMP_DIR, VALIDATION_CLEAN_CSV_NAME)
    df.to_csv(out_path_validation, index=False)

    # Registro del evento
    logger.info("###DataFrame leído y guardado en %s con %d filas.", out_path_validation, len(df))
    
    return out_path_validation  # Almacenamiento del archivo al path en XCom

    
# Lee el csv almacenado en Xcom y lo inserta en Postgres para archivos 2012-#.csv
##La conexion de postgre fue configurada en UI de Airflow, por medio del conector de postgres disponible para Airflow
###Parametros:
### Connection ID = postgres_local
### Connection Type = Postgres
### Host = localhost
### Login = user
### Password = *******
### Port = 5432
### Schema = test_database

def load_data_to_postgres(ti):
    # Recupera la ruta del CSV devuelta por la tarea anterior
    tmp_path = ti.xcom_pull(task_ids="read_csv")
    if not tmp_path or not os.path.isfile(tmp_path):
        raise FileNotFoundError(f"###No se encontró el archivo temporal: {tmp_path}")
    # Carga del archivo a un df para procesarse
    df = pd.read_csv(tmp_path)

    # Reconversión de seguridad (si el CSV llega con strings)
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["price"]     = pd.to_numeric(df["price"], errors="coerce")
    df["user_id"]   = pd.to_numeric(df["user_id"], errors="coerce")

    # Reemplazar NaN/NaT por None para que Postgres inserte NULL
    df = df.replace({np.nan: None})

    # Convertir numpy a tipos nativos de Python (int, float, None, datetime)
    def to_python_type(v):
        if pd.isna(v):
            return None
        # numpy genéricos a Python
        if hasattr(v, "item"):
            try:
                return v.item()
            except Exception:
                pass
        return v
    
    # Lista que se usara para insertar los datos a la DB
    rows = [
        tuple(to_python_type(v) for v in row)
        for row in df[["timestamp", "price", "user_id"]].itertuples(index=False, name=None)
    ]
    # Conexion a postgres
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    conn.autocommit = False

    with conn, conn.cursor() as cur:
        execute_values(
            cur,
            # Insercion de registros a la tabla postgres
            f'INSERT INTO {TABLE_NAME} ("timestamp", price, user_id) VALUES %s',
            rows,
            page_size=50 # Lote
        )

        # Consulta de resgistros cargados a la DB, para extraer estadisticas para archivos 2012-#.csv
        cur.execute(f"""
            SELECT 
                COUNT(*) AS total_filas,
                SUM(price)   AS suma_precios,
                (SUM(price) / NULLIF(COUNT(price), 0)) AS promedio_precios,
                MIN(price)   AS precio_minimo,
                MAX(price)   AS precio_maximo
            FROM {TABLE_NAME};
        """)
        # Variables locales para estadisticas
        count, sum_price, avg_price, min_price, max_price = cur.fetchone()

        # Registro de eventos para Count, Sum, Promedio, Max, Min para Price
        logger.info("###DATA###")
        logger.info("###Estadísticas actuales en la tabla %s:", TABLE_NAME)
        logger.info("   -Total de filas: %d", count)
        logger.info("   -Suma total de precios: %.2f", sum_price)
        logger.info("   -Promedio de precios (SUM/COUNT): %.2f", avg_price)
        logger.info("   -Precio mínimo: %.2f", min_price)
        logger.info("   -Precio máximo: %.2f", max_price)


# Lee el csv almacenado en Xcom y lo inserta en Postgres para archivos validation.csv
def load_validation_data_to_postgres(ti):
    # Recupera la ruta del CSV devuelta por tarea anterior
    tmp_path = ti.xcom_pull(task_ids="read_validation_csv")
    if not tmp_path or not os.path.isfile(tmp_path):
        raise FileNotFoundError(f"###No se encontró el archivo temporal: {tmp_path}")
    # Carga del archivo a un df para procesarse
    df = pd.read_csv(tmp_path)

    # Reconversión de seguridad (si el CSV llega con strings)
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["price"]     = pd.to_numeric(df["price"], errors="coerce")
    df["user_id"]   = pd.to_numeric(df["user_id"], errors="coerce")

    # Reemplazar NaN/NaT por None para que Postgres inserte NULL
    df = df.replace({np.nan: None})

    # Convertir numpy a tipos nativos de Python (int, float, None, datetime)
    def to_python_type(v):
        if pd.isna(v):
            return None
        # numpy genéricos a Python
        if hasattr(v, "item"):
            try:
                return v.item()
            except Exception:
                pass
        return v
    
    # Lista que se usara para insertar los datos a la DB
    rows = [
        tuple(to_python_type(v) for v in row)
        for row in df[["timestamp", "price", "user_id"]].itertuples(index=False, name=None)
    ]
    
    # Conexion a postgres
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    conn.autocommit = False

    with conn, conn.cursor() as cur:
        execute_values(
            cur,
            # Insercion de registros a la tabla postgres
            f'INSERT INTO {VALIDATION_TABLE_NAME} ("timestamp", price, user_id) VALUES %s',
            rows,
            page_size=100
        )

        # Consulta de resgistros cargados a la DB, para extraer estadisticas para archivos validation.csv
        cur.execute(f"""
            SELECT 
                COUNT(*) AS total_filas,
                SUM(price)   AS suma_precios,
                (SUM(price) / NULLIF(COUNT(price), 0)) AS promedio_precios,
                MIN(price)   AS precio_minimo,
                MAX(price)   AS precio_maximo
            FROM {VALIDATION_TABLE_NAME};
        """)
        # Variables locales para estadisticas
        count, sum_price, avg_price, min_price, max_price = cur.fetchone()
        # Registro de eventos para Count, Sum, Promedio, Max, Min para Price
        logger.info("###VALIDATION DATA###")
        logger.info("###Estadísticas actuales en la tabla %s:", VALIDATION_TABLE_NAME)
        logger.info("   -Total de filas: %d", count)
        logger.info("   -Suma total de precios: %.2f", sum_price)
        logger.info("   -Promedio de precios (SUM/COUNT): %.2f", avg_price)
        logger.info("   -Precio mínimo: %.2f", min_price)
        logger.info("   -Precio máximo: %.2f", max_price)
    
# Definicion del DAG
with DAG(
    dag_id="micro_batch_pipeline",
    description="micro batch pipeline",
    start_date=datetime(2025, 10, 23),
    schedule="*/2 * * * *", # El DAG se ejecutara cada 2 min despues de que sea inicializado (Simulando un Microbatch processing)
    catchup=False,
) as dag:

    # Definicion de tareas
    ## Leer, procesar y guarda un CSV temporal. Retorna la RUTA (XCom) para archivos 2012-#.csv
    read_csv = PythonOperator(
        task_id="read_csv",
        python_callable=read_data,
    )
    ## Leer, procesar y guarda un CSV temporal. Retorna la RUTA (XCom) para archivos validation.csv
    read_validation_csv = PythonOperator(
        task_id="read_validation_csv",
        python_callable=read_validation_data,
    )
    ## Lee el csv almacenado en Xcom y lo inserta en Postgres para archivos 2012-#.csv
    load_postgres = PythonOperator(
        task_id="load_postgres",
        python_callable=load_data_to_postgres,  
    )
    ## Lee el csv almacenado en Xcom y lo inserta en Postgres para archivos validation.csv
    load_validation_data_postgres = PythonOperator(
        task_id="load_validation_data_postgres",
        python_callable=load_validation_data_to_postgres,  
    )

    # Definicon de dependencias
    read_csv >> read_validation_csv >> load_postgres >> load_validation_data_postgres