import os
import sys
from datetime import datetime, timedelta
import subprocess

ROOT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if ROOT_PATH not in sys.path:
    sys.path.insert(0, ROOT_PATH)

from google.cloud import storage
from src.extraction.common.bling_api_client import BlingClient
from src.extraction import sales
from src.extraction.common.secret_manager import SecretManagerStateManager

def run_weekly_extraction(project_id: str, bucket_name: str, secret_id: str):
    state_manager = SecretManagerStateManager(project_id=project_id, secret_id=secret_id)
    cloud_storage_client = storage.Client(project=project_id)
    bucket = cloud_storage_client.bucket(bucket_name)
    client = BlingClient(state_manager=state_manager)

    dataFinal = datetime.today() - timedelta(days=1)
    dataInicial = dataFinal - timedelta(days=6)
    
    print(f"Período de extração: de {dataInicial.strftime('%d/%m/%Y')} a {dataFinal.strftime('%d/%m/%Y')}")

    sales.sales_extraction(
        client=client,
        dataInicial=dataInicial.strftime('%Y-%m-%d'),
        dataFinal=dataFinal.strftime('%Y-%m-%d'),
        storage_bucket=bucket
    )

def run_transformation(dbt_project_path: str):
    command = ["dbt", "run", "--select", "tag:semanal", "--target", "prod"]

    try:
        subprocess.run(
            command,
            cwd=dbt_project_path,
            check=True,
            capture_output=True,
            text=True
        )

        print("✅ Transformação com dbt concluída com sucesso!")

    except subprocess.CalledProcessError as e:
        print("\n" + "="*80, file=sys.stderr)
        print("🚨 ERRO FATAL NA EXECUÇÃO DO DBT! 🚨", file=sys.stderr)
        print("="*80 + "\n", file=sys.stderr)
        
        print("--- CAUSA RAIZ (saída de erro do dbt - stderr) ---", file=sys.stderr)
        print(e.stderr, file=sys.stderr)
        print("--------------------------------------------------\n", file=sys.stderr)

        print("--- STDOUT ---", file=sys.stderr)
        print(e.stdout, file=sys.stderr)
        print("--------------------------------------------------\n", file=sys.stderr)
        
        raise e


if __name__ == "__main__":
    PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
    BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME")
    SECRET_ID = os.environ.get("SECRET_ID_BLING")
    DBT_PROJECT_PATH = "/app/dbt_project"
    DBT_PROFILES_DIR = "/app"

    try:
        print("Pipeline semanal de dados do Bling iniciada.")
        run_weekly_extraction(PROJECT_ID, BUCKET_NAME, SECRET_ID)
        run_transformation(DBT_PROJECT_PATH)
        print("Pipeline concluída com sucesso!")
    except Exception as e:
        print(f"Pipeline falhou com o erro: {e}", file=sys.stderr)
        sys.exit(1)