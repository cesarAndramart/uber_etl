from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.utils.dates import days_ago

# Configuración básica
PROJECT_ID = "project_name"
REGION = "region_name"
CLUSTER_NAME = "dataproc_cluster_name"
SCRIPT_URI = "script_URI"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 0,
}

with DAG(
    "composer_dataproc_dag",
    default_args=default_args,
    description="Ejecutar el script de python de transformación desde airflow para un cluster de dataproc",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Configuración del trabajo de Dataproc
    dataproc_job_config = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": SCRIPT_URI,  # Ruta al script Python en GCS
            "args": ["--argumento1", "valor1", "--argumento2", "valor2"],  # Argumentos opcionales
        },
    }

    # Operador para enviar el trabajo a Dataproc
    run_dataproc_job = DataprocSubmitJobOperator(
        task_id="run_dataproc_python_script",
        job=dataproc_job_config,
        region=REGION,
        project_id=PROJECT_ID,
        gcp_conn_id="google_cloud_default",  # Conexión predeterminada en Composer
    )

    run_dataproc_job