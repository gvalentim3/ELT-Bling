from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import requests
from typing import Dict, Any, List, Optional
import os
import sys

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

def save_raw_product_categories(data: Dict[str, Any], output_dir: Path) -> None:
    output_file = output_dir / Path("raw_product_categories.json")
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Salvando dados de categorias de produtos em: {output_file}!")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def extract_product_categories(client: BlingClient, output_dir: Path) -> Optional[List[Dict[str, Any]]]:
    try:
        logger.info("Extraindo as categorias de produtos no Bling!")
        response = client.get(endpoint="categorias/produtos")

        data = response.json()

        consolidated_data = consolidate_product_categories_results(data=data.get('data', []))
    
        save_raw_product_categories(data=consolidated_data, output_dir=output_dir)

    except requests.exceptions.RequestException as e:
        logger.error(f"Erro ao extrair categorias de produtos: {e}")
        return None