from typing import Dict, List
import requests
from threading import Lock, Semaphore
from collections import deque
import concurrent.futures
import logging
from time import sleep
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime, timedelta

from extraction.common.bling_api_client import BlingClient

logger = logging.getLogger(__name__)

class ProgressTracker:
    def __init__(self, total_batches: int, total_ids: int):
        self.total_batches = total_batches
        self.total_ids = total_ids
        self.completed_batches = 0
        self.successful_ids = 0
        self.failed_ids = 0
        self.start_time = time.time()
        self.lock = Lock()
        
    def update_batch(self, success_count: int, failed_count: int, batch_name: str):
        with self.lock:
            self.completed_batches += 1
            self.successful_ids += success_count
            self.failed_ids += failed_count
            
            processed_ids = self.successful_ids + self.failed_ids
            
            batch_progress = (self.completed_batches / self.total_batches) * 100
            id_progress = (processed_ids / self.total_ids) * 100
            
            progress_bar = self._create_progress_bar(batch_progress)
            
            print(f"\r{progress_bar} | "
                  f"Lotes: {self.completed_batches}/{self.total_batches} ({batch_progress:.1f}%) | "
                  f"IDs: {processed_ids}/{self.total_ids} ({id_progress:.1f}%) | "
                  f"Sucesso: {self.successful_ids} | Falhas: {self.failed_ids} | ")
            
            logger.info(f"✓ Batch '{batch_name}': {success_count} success, {failed_count} failed")
    
    def _create_progress_bar(self, percentage: float, width: int = 30) -> str:
        filled = int(width * percentage / 100)
        bar = '█' * filled + '░' * (width - filled)
        return f"[{bar}] {percentage:.1f}%"
    
    def _format_time(self, seconds: float) -> str:
        if seconds < 60:
            return f"{seconds:.0f}s"
        elif seconds < 3600:
            return f"{seconds/60:.0f}m {seconds%60:.0f}s"
        else:
            hours = seconds // 3600
            minutes = (seconds % 3600) // 60
            return f"{hours:.0f}h {minutes:.0f}m"
    
    def final_report(self):
        total_time = time.time() - self.start_time
        print("\n" + "="*100)
        print("Extração completa!")
        print("="*100)
        print(f"Tempo total: {self._format_time(total_time)}")
        print(f"Total de lotes extraídos: {self.completed_batches}/{self.total_batches}")
        print(f"Total de IDs extraídos: {self.successful_ids + self.failed_ids}/{self.total_ids}")
        print(f"IDs extraídos com sucesso: {self.successful_ids} ({(self.successful_ids/self.total_ids)*100:.1f}%)")
        print(f"Extrações com falha: {self.failed_ids} ({(self.failed_ids/self.total_ids)*100:.1f}%)")
        print(f"Taxa de sucesso de extração: {(self.successful_ids/(self.successful_ids + self.failed_ids))*100:.2f}%")
        print("="*100)

class RateLimitedExecutor:
    def __init__(self, max_workers: int, reqs_per_second: int):
        self.max_workers = max_workers
        self.reqs_per_second = reqs_per_second
        self.request_times = deque()
        self.lock = Lock()
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)

    def _wait_for_rate_limit(self):
        with self.lock:
            now = time.time()
            while self.request_times and now - self.request_times[0] >= 1.0:
                self.request_times.popleft()
            
            if len(self.request_times) >= self.reqs_per_second:
                sleep_time = 1.0 - (now - self.request_times[0]) + 0.01
                if sleep_time > 0:
                    sleep(sleep_time)
                    now = time.time()
                    while self.request_times and now - self.request_times[0] >= 1.0:
                        self.request_times.popleft()
            
            self.request_times.append(now)

    def submit(self, fn, *args, **kwargs):
        def wrapped_fn():
            self._wait_for_rate_limit()
            return fn(*args, **kwargs)
        return self.executor.submit(wrapped_fn)

def process_batch(client: BlingClient, endpoint: str, id_batch: List[str], batch_name: str) -> Dict:
    results = {'success': [], 'failed': [], 'batch_name': batch_name}
    
    for object_id in id_batch:
        try:
            full_endpoint = f"{endpoint}/{object_id}"
                
            response = client.get(endpoint=full_endpoint)

            results['success'].append(response.json())
                
        except requests.exceptions.RequestException as e:
            logger.warning(f"Request failed for ID {object_id}: {str(e)[:100]}...")    
            results['failed'].append(object_id)
        except Exception as e:
            logger.error(f"Unexpected error with ID {object_id}: {e}")
            results['failed'].append(object_id)

    return results

def process_pre_batched(
    batched_dict: Dict[str, List[str]], 
    endpoint: str, 
    client: BlingClient,
    max_workers: int = 3,
    reqs_per_second: int = 3,
    show_progress: bool = True
) -> Dict:
    
    total_batches = len(batched_dict)
    total_ids = sum(len(batch) for batch in batched_dict.values())
    
    if show_progress:
        progress_tracker = ProgressTracker(total_batches, total_ids)
        print("\n" + "="*100)
        print(f"Iniciando extração em lotes")
        print("="*100)
        print(f"📊 Total de lotes: {total_batches}")
        print(f"📊 Total de IDs: {total_ids:,}")
        print(f"⏱️  Tempo estimado: {total_ids/reqs_per_second/60:.1f} minutos (mínimo)")
        print("="*100)
        print()
    
    executor = RateLimitedExecutor(min(max_workers, 3), reqs_per_second)
    futures = {}
    results = {}
    
    for batch_name, id_batch in batched_dict.items():
        future = executor.submit(
            process_batch,
            id_batch=id_batch,
            endpoint=endpoint,
            client=client,
            batch_name=batch_name
        )
        futures[future] = batch_name
        results[batch_name] = {'success': [], 'failed': []}

    for future in concurrent.futures.as_completed(futures):
        batch_name = futures[future]
        
        try:
            batch_result = future.result()
            results[batch_name] = {
                'success': batch_result['success'],
                'failed': batch_result['failed']
            }
            
            success_count = len(batch_result['success'])
            failed_count = len(batch_result['failed'])
            
            if show_progress:
                progress_tracker.update_batch(success_count, failed_count, batch_name)
            else:
                logger.info(f"Batch {batch_name} completed: {success_count} success, {failed_count} failed")
            
        except Exception as e:
            logger.error(f"Catastrophic failure in batch {batch_name}: {e}")
            batch_size = len(batched_dict[batch_name])
            results[batch_name] = {
                'success': [], 
                'failed': batched_dict[batch_name]
            }
            
            if show_progress:
                progress_tracker.update_batch(0, batch_size, batch_name)
    
    executor.executor.shutdown(wait=True)
    
    if show_progress:
        progress_tracker.final_report()
    
    return results