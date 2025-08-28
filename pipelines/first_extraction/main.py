import subprocess
import sys
import os
from google.cloud import storage

ROOT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if ROOT_PATH not in sys.path:
    sys.path.insert(0, ROOT_PATH)

from src.extraction.common.bling_api_client import BlingClient
from src.extraction import sales, sales_channels, products, product_categories, sales_status
from src.extraction.common.secret_manager import SecretManagerStateManager

def extraction(project_id: str, bucket_name: str, secret_id: str):
    state_manager = SecretManagerStateManager(project_id=project_id, secret_id=secret_id)
    cloud_storage_client = storage.Client(project=project_id)
    bucket = cloud_storage_client.bucket(bucket_name)
    client = BlingClient(state_manager=state_manager)

    product_categories.extract_product_categories(client=client, storage_bucket=bucket)
    sales_channels.extract_sales_channels(client=client, storage_bucket=bucket)
    sales_status.extract_sales_status(client=client, storage_bucket=bucket)
    products.products_extraction(client=client, storage_bucket=bucket)    
    sales.sales_extraction(client=client, dataInicial="2024-01-01", dataFinal="2024-08-17", storage_bucket=bucket)

def run_transformation(dbt_project_path: str):
    command = ["dbt", "run"]

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
    DBT_PROJECT_PATH = "../ETL-BLING/dbt_project"

    if not all([PROJECT_ID, BUCKET_NAME, SECRET_ID]):
        print("ERRO: As variáveis de ambiente GCP_PROJECT_ID, GCS_BUCKET_NAME e SECRET_ID_BLING devem ser definidas.", file=sys.stderr)
        sys.exit(1)
        
    try:
        print("Pipeline de ETL iniciada.")
        extraction(PROJECT_ID, BUCKET_NAME, SECRET_ID)
        run_transformation(DBT_PROJECT_PATH)
        print("Pipeline de ETL concluída com sucesso!")
    except Exception as e:
        print(f"Pipeline falhou com o erro: {e}", file=sys.stderr)
        sys.exit(1)