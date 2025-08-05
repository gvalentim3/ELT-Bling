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
            
            elapsed_time = time.time() - self.start_time
            processed_ids = self.successful_ids + self.failed_ids
            
            batch_rate = self.completed_batches / elapsed_time if elapsed_time > 0 else 0
            id_rate = processed_ids / elapsed_time if elapsed_time > 0 else 0
            
            remaining_batches = self.total_batches - self.completed_batches
            eta_seconds = remaining_batches / batch_rate if batch_rate > 0 else 0
            eta_time = datetime.now() + timedelta(seconds=eta_seconds)
            
            batch_progress = (self.completed_batches / self.total_batches) * 100
            id_progress = (processed_ids / self.total_ids) * 100
            
            elapsed_str = self._format_time(elapsed_time)
            eta_str = self._format_time(eta_seconds) if eta_seconds > 0 else "Unknown"
            
            progress_bar = self._create_progress_bar(batch_progress)
            
            print(f"\r{progress_bar} | "
                  f"Batches: {self.completed_batches}/{self.total_batches} ({batch_progress:.1f}%) | "
                  f"IDs: {processed_ids}/{self.total_ids} ({id_progress:.1f}%) | "
                  f"Success: {self.successful_ids} | Failed: {self.failed_ids} | "
                  f"Rate: {id_rate:.1f} ids/s | "
                  f"Elapsed: {elapsed_str} | ETA: {eta_str}", end='', flush=True)
            
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
        print("🎉 PROCESSING COMPLETE!")
        print("="*100)
        print(f"Total Time: {self._format_time(total_time)}")
        print(f"Total Batches Processed: {self.completed_batches}/{self.total_batches}")
        print(f"Total IDs Processed: {self.successful_ids + self.failed_ids}/{self.total_ids}")
        print(f"✅ Successful IDs: {self.successful_ids} ({(self.successful_ids/self.total_ids)*100:.1f}%)")
        print(f"❌ Failed IDs: {self.failed_ids} ({(self.failed_ids/self.total_ids)*100:.1f}%)")
        print(f"📊 Success Rate: {(self.successful_ids/(self.successful_ids + self.failed_ids))*100:.2f}%")
        print(f"⚡ Average Rate: {(self.successful_ids + self.failed_ids)/total_time:.2f} IDs/second")
        print("="*100)

class RateLimitedExecutor:
    def __init__(self, max_workers: int, reqs_per_second: int):
        self.max_workers = max_workers
        self.reqs_per_second = reqs_per_second
        self.request_times = deque()
        self.lock = Lock()
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        self.semaphore = Semaphore(reqs_per_second)

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

def create_session() -> requests.Session:
    session = requests.Session()
    
    retry_strategy = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=10,
        pool_maxsize=10,
        pool_block=False
    )
    
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session

def process_batch(id_batch: List[str], base_url: str, headers: Dict, batch_name: str) -> Dict:
    results = {'success': [], 'failed': [], 'batch_name': batch_name}
    
    session = create_session()
    
    try:
        for i, order_id in enumerate(id_batch):
            try:
                item_url = f"{base_url}/{order_id}"
                
                response = session.get(
                    item_url,
                    headers=headers,
                    timeout=10
                )
                response.raise_for_status()
                results['success'].append(response.json())
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request failed for ID {order_id}: {str(e)[:100]}...")
                results['failed'].append(order_id)
            except Exception as e:
                logger.error(f"Unexpected error with ID {order_id}: {e}")
                results['failed'].append(order_id)
                
            if len(id_batch) > 50 and (i + 1) % 25 == 0:
                logger.debug(f"Batch {batch_name}: {i+1}/{len(id_batch)} IDs processed")
                
    finally:
        session.close()
    
    return results

def process_pre_batched(
    batched_dict: Dict[str, List[str]], 
    url: str, 
    headers: Dict,
    max_workers: int = 3,
    reqs_per_second: int = 3,
    show_progress: bool = True
) -> Dict:
    
    total_batches = len(batched_dict)
    total_ids = sum(len(batch) for batch in batched_dict.values())
    
    if show_progress:
        progress_tracker = ProgressTracker(total_batches, total_ids)
        print("\n" + "="*100)
        print(f"🚀 STARTING ETL EXTRACTION")
        print("="*100)
        print(f"📊 Total Batches: {total_batches}")
        print(f"📊 Total IDs: {total_ids:,}")
        print(f"⚙️  Workers: {min(max_workers, 3)}")
        print(f"⚙️  Rate Limit: {reqs_per_second} req/sec")
        print(f"⏱️  Estimated Time: {total_ids/reqs_per_second/60:.1f} minutes (minimum)")
        print("="*100)
        print()
    
    executor = RateLimitedExecutor(min(max_workers, 3), reqs_per_second)
    futures = {}
    results = {}
    
    for batch_name, id_batch in batched_dict.items():
        future = executor.submit(
            process_batch,
            id_batch=id_batch,
            base_url=url,
            headers=headers,
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