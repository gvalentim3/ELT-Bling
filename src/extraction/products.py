from datetime import datetime, timezone
import sys
from typing import Dict, List, Any
import logging
from venv import logger
import json
from pathlib import Path
import os

ROOT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if ROOT_PATH not in sys.path:
    sys.path.append(ROOT_PATH)

from extraction.common.concurrency import process_pre_batched
from extraction.common.bling_api_client import BlingClient

logger = logging.getLogger(__name__)

def extract_all_products_ids(client: BlingClient, initial_params: Dict[str, str]) -> Dict[int, List[str]]:
    current_page = 1
    all_products_ids = {}
    limit = initial_params.get('limite', 100)
    start_time = datetime.now(timezone.utc)
    collected_ids = 0

    while True:
        params = initial_params.copy()
        params['pagina'] = current_page

        response = client.get(endpoint="produtos", params=params)

        response.raise_for_status()
        data = response.json()

        if not data.get('data'):
            break

        page_ids = [product['id'] for product in data['data']]
        all_products_ids[current_page] = page_ids

        collected_ids += len(page_ids)
        
        if len(data['data']) < limit:
            break

        current_page += 1

    duration = (datetime.now(timezone.utc) - start_time).total_seconds()
    logger.info(f"\nExtração de {collected_ids} IDs de produtos completa em {duration:.2f} segundos")

    return all_products_ids

def consolidate_results(results: Dict, params: Dict) -> Dict[str, Any]:
    consolidated = {
        "metadata": {
            "extraction_timestamp": datetime.now().isoformat(),
            "extraction_params": params,
            "successful_extractions": 0,
            "failed_extractions": 0,
            "batches_processed": len(results)
        },
        "products": [],
        "processing_summary": {}
    }
    
    for batch_name, batch_result in results.items():
        batch_summary = {
            "successful_count": len(batch_result['success']),
            "failed_count": len(batch_result['failed']),
            "failed_ids": batch_result['failed']
        }
        
        consolidated["processing_summary"][batch_name] = batch_summary
        consolidated["metadata"]["successful_extractions"] += batch_summary["successful_count"]
        consolidated["metadata"]["failed_extractions"] += batch_summary["failed_count"]
        
        for products_data in batch_result['success']:
            consolidated["products"].append(products_data)
    
    consolidated["metadata"]["total_products"] = len(consolidated["products"])

    return consolidated

def handle_requests(client: BlingClient, endpoint: str, ids_dict: Dict[str, str], params: Dict[str, str]):
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
        
    try:
        results = process_pre_batched(
            batched_dict=ids_dict, 
            endpoint=endpoint, 
            client=client,
            max_workers=3,
            reqs_per_second=3,
            show_progress=True
        )

        consolidated_data = consolidate_results(results, params)

        return consolidated_data
        
    except Exception as e:
        logger.error(f"Erro: {e}")
        sys.exit(1)

def save_raw_sales_products(data: Dict[str, Any], output_dir: Path):
    output_file = output_dir / f"raw_products.json"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
 
def products_extraction(client: BlingClient):
    logger.info("Iniciando a extração dos dados de produtos do Bling!")

    endpoint="produtos"

    params = {
        "limite": 100
    }

    ids_dict = extract_all_products_ids(client=client, endpoint=endpoint, initial_params=params)

    data = handle_requests(client=client, endpoint=endpoint, ids_dict=ids_dict, params=params)

    save_raw_sales_products(data, Path("data/raw"), params=params)