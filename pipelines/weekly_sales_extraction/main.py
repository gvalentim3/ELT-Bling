import os
import sys
from datetime import datetime, timedelta

ROOT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if ROOT_PATH not in sys.path:
    sys.path.insert(0, ROOT_PATH)

from flask import Flask, request
from google.cloud import storage
from src.extraction.common.bling_api_client import BlingClient
from src.extraction import sales
from src.extraction.common.state_manager import FileStateManager

app = Flask(__name__)

def run_weekly_extraction():
    state_manager = FileStateManager()
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


# --- Endpoint da API ---
@app.route("/", methods=["POST"])
def trigger_extraction():
    try:
        run_weekly_extraction()
        return "Pipeline de extração de vendas executada com sucesso!", 200
    except Exception as e:
        print(f"🚨 Erro na execução da pipeline: {e}", file=sys.stderr)
        return f"Erro interno no servidor: {e}", 500

extraction()