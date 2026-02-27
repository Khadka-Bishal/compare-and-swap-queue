import os
import time
import threading
from src.queue.service import QueueClient
from src.queue.service import BufferedQueueClient

def run_worker(client: BufferedQueueClient, worker_id: str, num_jobs: int):
    # Each worker pushes and claims jobs rapidly
    for i in range(num_jobs):
        client.push({"task": "heavy_compute", "id": i, "worker_id": worker_id})
        job = client.claim(worker_id)
        if job:
            client.ack(job["id"], worker_id)

def main():
    # Clean up old queue files
    for ext in ['json', 'meta', 'lock']:
        try:
            os.remove(f"queue.{ext}")
        except FileNotFoundError:
            pass

    # Initialize CAS Queue and the Buffered Wrapper
    # Flush every 50ms, or when we hit 20 pending intents
    base_client = QueueClient("queue")
    buffered_client = BufferedQueueClient(base_client, max_batch_size=20, flush_interval_ms=50)

    print("Starting many concurrent workers submitting intents...")
    start_time = time.time()
    
    threads = []
    # 5 threads, 100 loops each = 500 pushes, 500 claims, 500 acks = 1500 intents
    for i in range(5):
        t = threading.Thread(target=run_worker, args=(buffered_client, f"worker_{i}", 100))
        threads.append(t)
        t.start()
        
    for t in threads:
        t.join()
        
    buffered_client.shutdown()
    
    elapsed = time.time() - start_time
    metrics = buffered_client.metrics
    
    print("-" * 40)
    print(f"Finished 1500 intent operations in {elapsed:.2f} seconds.")
    print(f"Total CAS Flushes (Writes to Disk): {metrics['total_flushes']}")
    print(f"Total Intents Processed: {metrics['total_intents']}")
    print(f"Average Batch Size: {metrics['total_intents'] / max(1, metrics['total_flushes']):.1f} intents/write")
    
    final_data, version = base_client.cas.read()
    print(f"Final internal file version: {version}")

if __name__ == "__main__":
    main()
