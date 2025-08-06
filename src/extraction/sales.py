from datetime import datetime, timezone
import sys
from typing import Dict, List, Any
import logging
from venv import logger
import json
from pathlib import Path
import os
import re

ROOT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if ROOT_PATH not in sys.path:
    sys.path.append(ROOT_PATH)

from extraction.common.concurrency import process_pre_batched
from extraction.common.bling_api_client import BlingClient

logger = logging.getLogger(__name__)

def extract_all_sales_orders_ids(client: BlingClient, endpoint: str, initial_params: Dict[str, str]) -> Dict[int, List[str]]:
    current_page = 1
    all_orders_ids = {}
    limit = initial_params.get('limite', 100)
    start_time = datetime.now(timezone.utc)
    collected_ids = 0

    while True:
        params = initial_params.copy()
        params['pagina'] = current_page

        response = client.get(endpoint=endpoint, params=params)

        response.raise_for_status()
        data = response.json()

        if not data.get('data'):
            break

        page_ids = [order['id'] for order in data['data']]
        all_orders_ids[current_page] = page_ids

        collected_ids += len(page_ids)
        
        if len(data['data']) < limit:
            break

        current_page += 1

    duration = (datetime.now(timezone.utc) - start_time).total_seconds()
    logger.info(f"\nExtração de {collected_ids} IDs de venda completa em {duration:.2f} segundos")

    return all_orders_ids

def consolidate_results(results: Dict, params: Dict) -> Dict[str, Any]:
    consolidated = {
        "metadata": {
            "extraction_timestamp": datetime.now().isoformat(),
            "extraction_params": params,
            "successful_extractions": 0,
            "failed_extractions": 0,
            "batches_processed": len(results)
        },
        "orders": [],
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
        
        for order_data in batch_result['success']:
            consolidated["orders"].append(order_data)
    
    consolidated["metadata"]["total_orders"] = len(consolidated["orders"])

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

def save_raw_sales_orders(data: Dict[str, Any], output_dir: Path, params: Dict[str, str] = None):
    start_date = params.get('dataInicial').replace("-", "")
    end_date = params.get('dataFinal').replace("-", "")
    
    output_file = output_dir / f"raw_sales_orders_{start_date}_{end_date}.json"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
 
def sales_extraction(dataInicial: str, dataFinal: str, client: BlingClient, headers: Dict[str, str]):
    """
    dataInicial and dataFinal are always expected in the YYYY-MM-DD format.
    """
    
    date_pattern = r'^\d{4}-\d{2}-\d{2}$'

    if not re.match(date_pattern, dataInicial):
        raise ValueError(f"dataInicial must be in YYYY-MM-DD format. Received: {dataInicial}")
    
    if not re.match(date_pattern, dataFinal):
        raise ValueError(f"dataFinal must be in YYYY-MM-DD format. Received: {dataFinal}")

    logger.info("Iniciando a extração dos dados de venda do Bling!")

    endpoint="pedidos/vendas"

    params = {
        "limite": 100,
        "dataInicial": dataInicial,
        "dataFinal": dataFinal
    }

    ids_dict = extract_all_sales_orders_ids(client=client, endpoint=endpoint, initial_params=params)

    data = handle_requests(client=client, endpoint=endpoint, ids_dict=ids_dict, params=params)

    save_raw_sales_orders(data, Path("data/raw"), params=params)