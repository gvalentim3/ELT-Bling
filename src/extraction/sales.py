from datetime import datetime, timezone
import sys
import time
from typing import Dict, List, Any
import logging
import json
import os
import re
from google.cloud.storage import Bucket

ROOT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if ROOT_PATH not in sys.path:
    sys.path.insert(0, ROOT_PATH)

from .common.concurrency import process_pre_batched
from .common.bling_api_client import BlingClient

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

def retry_failed_ids(client: BlingClient, endpoint: str, failed_ids: List[str], params: Dict[str, str], max_retries: int = 3) -> Dict[str, Any]:
    retry_results = {
        "success": [],
        "failed": [],
        "retry_summary": {
            "total_retried": len(failed_ids),
            "successful_retries": 0,
            "permanent_failures": 0
        }
    }
    
    if not failed_ids:
        return retry_results
    
    logger.info(f"Iniciando retry sequencial de {len(failed_ids)} IDs falhados...")
    
    for product_id in failed_ids:
        retry_count = 0
        success = False
        
        while retry_count < max_retries and not success:
            try:
                if retry_count > 0:
                    wait_time = 2 ** retry_count
                    logger.info(f"Aguardando {wait_time}s antes do retry {retry_count + 1} para ID {product_id}")
                    time.sleep(wait_time)
                
                response = client.get(endpoint=f"{endpoint}/{product_id}", params=params)
                response.raise_for_status()
                
                product_data = response.json()
                retry_results["success"].append(product_data)
                retry_results["retry_summary"]["successful_retries"] += 1
                
                logger.info(f"Retry bem-sucedido para ID {product_id} na tentativa {retry_count + 1}")
                success = True
                
            except Exception as e:
                retry_count += 1
                logger.warning(f"Retry {retry_count}/{max_retries} falhado para ID {product_id}: {str(e)}")
                
                if retry_count >= max_retries:
                    retry_results["failed"].append(product_id)
                    retry_results["retry_summary"]["permanent_failures"] += 1
                    logger.error(f"ID {product_id} falhou permanentemente após {max_retries} tentativas")
    
    logger.info(f"Retry concluído: {retry_results['retry_summary']['successful_retries']} sucessos, "
                f"{retry_results['retry_summary']['permanent_failures']} falhas permanentes")
    
    return retry_results

def consolidate_results(results: Dict, params: Dict, client: BlingClient = None, endpoint: str = None) -> Dict[str, Any]:
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
    
    all_failed_ids = []
    
    for batch_name, batch_result in results.items():
        batch_summary = {
            "successful_count": len(batch_result['success']),
            "failed_count": len(batch_result['failed']),
            "failed_ids": batch_result['failed']
        }
        
        consolidated["processing_summary"][batch_name] = batch_summary
        consolidated["metadata"]["successful_extractions"] += batch_summary["successful_count"]
        consolidated["metadata"]["failed_extractions"] += batch_summary["failed_count"]
        
        for orders_data in batch_result['success']:
            consolidated["orders"].append(orders_data)

        all_failed_ids.extend(batch_result['failed'])
    
    if all_failed_ids and client and endpoint:
        logger.info(f"Encontrados {len(all_failed_ids)} IDs falhados. Iniciando processo de retry...")
        
        retry_results = retry_failed_ids(
            client=client, 
            endpoint=endpoint, 
            failed_ids=all_failed_ids, 
            params=params,
            max_retries=3
        )
        
        consolidated["orders"].extend(retry_results["success"])
        
        consolidated["metadata"]["successful_extractions"] += retry_results["retry_summary"]["successful_retries"]
        consolidated["metadata"]["failed_extractions"] = (
            consolidated["metadata"]["failed_extractions"] - 
            retry_results["retry_summary"]["successful_retries"] + 
            retry_results["retry_summary"]["permanent_failures"]
        )
        
        consolidated["metadata"]["retry_summary"] = retry_results["retry_summary"]
        
        for batch_name in consolidated["processing_summary"]:
            consolidated["processing_summary"][batch_name]["failed_ids"] = []
            consolidated["processing_summary"][batch_name]["failed_count"] = 0
        
        if retry_results["retry_summary"]["permanent_failures"] > 0:
            consolidated["processing_summary"]["permanent_failures"] = {
                "successful_count": 0,
                "failed_count": retry_results["retry_summary"]["permanent_failures"],
                "failed_ids": retry_results["failed"]
            }
    
    consolidated["metadata"]["total_orders"] = len(consolidated["orders"])
    
    logger.info(f"Consolidação completa: {consolidated['metadata']['total_orders']} produtos extraídos com sucesso")
    if consolidated["metadata"]["failed_extractions"] > 0:
        logger.warning(f"{consolidated['metadata']['failed_extractions']} extrações permanentemente falharam")

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

        consolidated_data = consolidate_results(
            results=results, 
            params=params, 
            client=client, 
            endpoint=endpoint
        )
        return consolidated_data
        
    except Exception as e:
        logger.error(f"Erro: {e}")
        sys.exit(1)

def save_raw_sales_orders_ndjson(data: Dict[str, Any], storage_bucket: Bucket, params: Dict[str, str] = None):
    records = data.get('orders', [])
    
    if not records:
        logger.warning("Nenhum registro encontrado na chave 'orders' para salvar.")
        return

    metadata = data.get("metadata", {})

    ndjson_lines = [json.dumps({"metadata": metadata}, ensure_ascii=False)]

    ndjson_lines.extend([json.dumps(record, ensure_ascii=False, separators=(',', ':')) for record in records])

    partition_date = params.get('dataFinal') if params else 'unknown_date'
    
    destination_blob_name = f"raw/sales_data/dt={partition_date}/raw_sales_orders.ndjson"
    blob = storage_bucket.blob(destination_blob_name)

    ndjson_content = "\n".join(ndjson_lines)

    blob.upload_from_string(
        data=ndjson_content,
        content_type="application/x-ndjson"
    )
    
    logger.info(f"Salvando dados de pedidos de venda em: gs://{storage_bucket.name}/{destination_blob_name}...")
 
def sales_extraction(client: BlingClient, dataInicial: str, dataFinal: str, storage_bucket: Bucket):
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

    save_raw_sales_orders_ndjson(data, storage_bucket=storage_bucket, params=params)