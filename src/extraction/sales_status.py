from datetime import datetime, timezone
import json
import logging
import requests
from typing import Dict, Any, List, Optional
import os
import sys
from google.cloud.storage import Bucket

ROOT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if ROOT_PATH not in sys.path:
    sys.path.append(ROOT_PATH)

from .common.bling_api_client import BlingClient

logger = logging.getLogger(__name__)

def consolidate_sales_status_results(data: List[Dict[str, Any]], params: Dict = {}) -> Dict[str, Any]:
    metadata = {
        "extraction_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "extraction_params": params,
        "total_records": len(data)
    }

    return {
        "metadata": metadata,
        "data": data
    }

def save_raw_sales_status_ndjson(data: Dict[str, Any], storage_bucket: Bucket) -> None:
    destination_blob_name = "raw/dim_data/raw_sales_status.ndjson"
    blob = storage_bucket.blob(destination_blob_name)

    ndjson_lines = []
    if "metadata" in data:
        ndjson_lines.append(json.dumps({"metadata": data["metadata"]}, ensure_ascii=False))
    
    for record in data.get("data", []):
        ndjson_lines.append(json.dumps(record, ensure_ascii=False))
    
    ndjson_string = "\n".join(ndjson_lines)
    blob.upload_from_string(ndjson_string, content_type="application/x-ndjson")
    
    logger.info(f"Salvando dados de status de venda em: gs://{storage_bucket.name}/{destination_blob_name}...")

def extract_sales_status(client: BlingClient, storage_bucket: Bucket) -> Optional[List[Dict[str, Any]]]:
    try:
        logger.info("Extraindo as status de venda no Bling!")
        response = client.get(endpoint="situacoes/modulos/98310")

        data = response.json()

        consolidated_data = consolidate_sales_status_results(data=data.get('data', []))
    
        save_raw_sales_status_ndjson(data=consolidated_data, storage_bucket=storage_bucket)

    except requests.exceptions.RequestException as e:
        logger.error(f"Erro ao extrair status de venda: {e}")
        return None