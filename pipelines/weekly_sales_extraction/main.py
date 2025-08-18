import os
import sys
from datetime import datetime, timedelta
import subprocess

ROOT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if ROOT_PATH not in sys.path:
    sys.path.insert(0, ROOT_PATH)

from flask import Flask, request
from google.cloud import storage
from src.extraction.common.bling_api_client import BlingClient
from src.extraction import sales
from src.extraction.common.secret_manager import SecretManagerStateManager

app = Flask(__name__)

def run_weekly_extraction():
    state_manager = SecretManagerStateManager(project_id="eletrofor", secret_id="bling-credentials")
    cloud_storage_client = storage.Client(project="eletrofor")
    bucket = cloud_storage_client.bucket("bling-raw-data")
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

def run_transformation():
    dbt_project_path = os.path.join(ROOT_PATH, "dbt_project")
    
    command = ["dbt", "run", "--select", "tag:vendas"]
    
    print(f"Executando comando dbt: {' '.join(command)}")

    result = subprocess.run(
        command,
        cwd=dbt_project_path,
        check=True,
        capture_output=True,
        text=True
    )

    print("--- Logs do dbt ---")
    print(result.stdout)
    print("-------------------")
    
    print("✅ Transformação com dbt (modelos 'semanal') concluída com sucesso!")


@app.route("/", methods=["POST"])
def trigger_etl():    
    try:
        run_weekly_extraction()
        run_transformation()
        return "Pipeline completa (Extração + dbt) executada com sucesso!", 200
    except subprocess.CalledProcessError as e:
        print(f"🚨 ERRO na execução do dbt!", file=sys.stderr)
        print(f"--- Output do dbt (stderr) ---", file=sys.stderr)
        print(e.stderr, file=sys.stderr) # Imprime o erro do dbt
        print(f"-----------------------------", file=sys.stderr)
        return f"Erro na etapa de transformação com dbt: {e.stderr}", 500
    except Exception as e:
        print(f"🚨 Erro na execução da pipeline: {e}", file=sys.stderr)
        return f"Erro interno no servidor: {e}", 500