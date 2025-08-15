from datetime import datetime, timezone
import sys
from typing import Dict, List, Any
import logging
from venv import logger
import json
import os
import time
from google.cloud.storage import Bucket

ROOT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if ROOT_PATH not in sys.path:
    sys.path.append(ROOT_PATH)

from .common.concurrency import process_pre_batched
from .common.bling_api_client import BlingClient

logger = logging.getLogger(__name__)

def extract_all_products_ids(client: BlingClient, endpoint: str, initial_params: Dict[str, str]) -> Dict[int, List[str]]:
    current_page = 1
    all_products_ids = {}
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

        page_ids = [product['id'] for product in data['data']]
        all_products_ids[current_page] = page_ids

        collected_ids += len(page_ids)
        
        if len(data['data']) < limit:
            break

        current_page += 1

    duration = (datetime.now(timezone.utc) - start_time).total_seconds()
    logger.info(f"\nExtração de {collected_ids} IDs de produtos completa em {duration:.2f} segundos")

    return all_products_ids

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
        "products": [],
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
        
        for products_data in batch_result['success']:
            consolidated["products"].append(products_data)

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
        
        consolidated["products"].extend(retry_results["success"])
        
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
    
    consolidated["metadata"]["total_products"] = len(consolidated["products"])
    
    logger.info(f"Consolidação completa: {consolidated['metadata']['total_products']} produtos extraídos com sucesso")
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

def save_raw_products(data: Dict[str, Any], storage_bucket: Bucket):
    destination_blob_name = "raw/products_data/raw_products.ndjson"

    blob = storage_bucket.blob(destination_blob_name)

    blob.upload_from_string(
        data=json.dumps(data, ensure_ascii=False, indent=4),
        content_type="application/json"
    )
    
    logger.info(f"Salvando dados de produtos em: gs://{storage_bucket.name}/{destination_blob_name}...")
 
def products_extraction(client: BlingClient, storage_bucket: Bucket):
    logger.info("Iniciando a extração dos dados de produtos do Bling!")

    endpoint="produtos"

    params = {
        "limite": 100
    }

    ids_dict = extract_all_products_ids(client=client, endpoint=endpoint, initial_params=params)

    data = handle_requests(client=client, endpoint=endpoint, ids_dict=ids_dict, params=params)

    save_raw_products(data=data, storage_bucket=storage_bucket)