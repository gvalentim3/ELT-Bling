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

def consolidate_product_categories_results(data: List[Dict[str, Any]], params: Dict = {}) -> Dict[str, Any]:
    metadata = {
        "extraction_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "extraction_params": params,
        "total_records": len(data)
    }

    return {
        "metadata": metadata,
        "data": data
    }

def save_raw_product_categories(data: Dict[str, Any], storage_bucket: Bucket) -> None:
    destination_blob_name = "raw/dim_data/raw_product_categories.json"

    blob = storage_bucket.blob(destination_blob_name)

    blob.upload_from_string(
        data=json.dumps(data, ensure_ascii=False, indent=4),
        content_type="application/json"
    )
    
    logger.info(f"Salvando dados de categorias de produtos em: gs://{storage_bucket.name}/{destination_blob_name}...")

def extract_product_categories(client: BlingClient, storage_bucket: Bucket) -> Optional[List[Dict[str, Any]]]:
    try:
        logger.info("Extraindo as categorias de produtos no Bling!")
        response = client.get(endpoint="categorias/produtos")

        data = response.json()

        consolidated_data = consolidate_product_categories_results(data=data.get('data', []))
    
        save_raw_product_categories(data=consolidated_data, storage_bucket=storage_bucket)

    except requests.exceptions.RequestException as e:
        logger.error(f"Erro ao extrair categorias de produtos: {e}")
        return None