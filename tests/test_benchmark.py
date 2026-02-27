import os
import time
import subprocess
from concurrent.futures import ThreadPoolExecutor

from src.queue.service import QueueClient
from src.client.smart_client import SmartQueueClient

# ---------------------------------------------
# Test Config
# ---------------------------------------------
WORKER_COUNT = 20
JOBS_PER_WORKER = 5
TOTAL_OPERATIONS = WORKER_COUNT * JOBS_PER_WORKER

def _clean_queue():
    for ext in ['json', 'meta', 'lock']:
        try: os.remove(f"queue.{ext}")
        except FileNotFoundError: pass

# ---------------------------------------------
# Pure CAS (Clients fighting for lock)
# ---------------------------------------------
def run_pure_cas():
    print(f"\n--- Running Pure CAS (Phase 1) with {WORKER_COUNT} workers ---")
    _clean_queue()
    client = QueueClient()
    
    def worker_task(worker_id):
        # Push jobs directly via CAS
        for i in range(JOBS_PER_WORKER):
            payload = {"task": f"job_{worker_id}_{i}"}
            client.push(payload)
            
    start_t = time.perf_counter()
    with ThreadPoolExecutor(max_workers=WORKER_COUNT) as executor:
        for w in range(WORKER_COUNT):
            executor.submit(worker_task, w)
            
    duration = time.perf_counter() - start_t
    print(f"Pure CAS Duration: {duration:.2f} seconds")
    print(f"Throughput: {TOTAL_OPERATIONS / duration:.2f} push/sec")

# ---------------------------------------------
#   Buffered Broker (Group Commits)
# ---------------------------------------------
def run_buffered_broker():
    print(f"\n--- Running Buffered Broker (Phase 4) with {WORKER_COUNT} workers ---")
    _clean_queue()
    
    # Start the broker in the background
    broker_proc = subprocess.Popen(
        [".venv/bin/uvicorn", "src.main:app", "--host", "127.0.0.1", "--port", "8000"],
        stdout=subprocess.DEVNULL, # keep console clean
        stderr=subprocess.DEVNULL
    )
    time.sleep(1) # wait for boot and registry
    
    smart_client = SmartQueueClient()
    
    def worker_task(worker_id):
        # Push jobs to HTTP api
        for i in range(JOBS_PER_WORKER):
            payload = {"task": f"job_{worker_id}_{i}"}
            smart_client.push(payload)
            
    start_t = time.perf_counter()
    with ThreadPoolExecutor(max_workers=WORKER_COUNT) as executor:
        for w in range(WORKER_COUNT):
            executor.submit(worker_task, w)
            
    duration = time.perf_counter() - start_t
    
    print(f"Buffered Broker Duration: {duration:.2f} seconds")
    print(f"Throughput: {TOTAL_OPERATIONS / duration:.2f} push/sec")
    
    # Print metrics collected by the broker
    metrics = smart_client._make_request("GET", "/metrics", retry_count=0).get("metrics")
    print("\nBroker Flush Metrics:")
    print(f"Total Intents Flushed: {metrics['total_intents']}")
    print(f"Total Flushes (Disk Writes): {metrics['total_flushes']}")
    avg = metrics['total_intents'] / max(1, metrics['total_flushes'])
    print(f"Avg Batch Size: {avg:.1f} intents/write")
    
    # Cleanup
    broker_proc.terminate()
    broker_proc.wait()

if __name__ == "__main__":
    run_pure_cas()
    run_buffered_broker()
