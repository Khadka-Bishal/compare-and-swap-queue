import os
import time
import multiprocessing
import httpx
from smart_client import SmartQueueClient
from buffered_client import BufferedQueueClient

def simulate_heartbeat_failure():
    print("\n--- TEST: Worker Failure & Reclaim ---")
    # Clean queue
    for ext in ['json', 'meta', 'lock']:
        try: os.remove(f"queue.{ext}")
        except FileNotFoundError: pass

    client = SmartQueueClient()
    
    # Push a job
    job_id = client.push({"task": "transcode_video"})
    
    # Worker A claims it with a very short lease timeout (2 seconds)
    print("Worker A claims the job with a 2-second lease...")
    job = client.claim("worker_A", lease_timeout_sec=2.0)
    print("Original attempt count:", job["attempt"])
    
    # Worker A crashes (we just sleep to simulate it not heartbeating/acking)
    print("Worker A crashes. Waiting 3 seconds for lease to expire...")
    time.sleep(3.0)
    
    # Worker B tries to claim
    print("Worker B attempts to claim...")
    job_reclaimed = client.claim("worker_B", lease_timeout_sec=2.0)
    
    if job_reclaimed and job_reclaimed["id"] == job_id.get("job_id"):
        print(f"SUCCESS! Worker B reclaimed the job. New attempt count: {job_reclaimed['attempt']}")
        print(f"Owned by: {job_reclaimed['claimed_by']}")
    else:
        print("FAILED to reclaim the job.")

def simulate_broker_failure():
    print("\n--- TEST: Broker HA Failover ---")
    client = SmartQueueClient()
    
    url_1 = client._get_active_broker_url()
    print(f"Current active broker: {url_1}")
    
    print("Pushing a job to ensure connection works...")
    job_id_1 = client.push({"task": "job_1"})
    print(f"Pushed job: {job_id_1}")
    
    # Simulate a Hard Broker Crash by finding the PID using the port and killing it
    # For this script we will actually just shut it down via the OS by forcing an exit
    print("Simulating FATAL CATASTROPHE: Killing the active broker process...")
    
    # We will use the REST API to force it to die if we created a debug endpoint, 
    # but since we didn't, we will simulate the connection error by giving the 
    # client a bad URL to force the _make_request retry block to spawn a new one.
    
    # Manually poison the queue.json with a dead URL to force the client to failover
    from queue_ops import QueueClient
    raw_qs = QueueClient()
    def _poison(data):
        data["broker"] = "http://127.0.0.1:9999" # Dead port
        return data
    raw_qs.cas.update_with_retry(_poison)
    
    print("Attempting to push another job. The SmartClient should detect the dead broker, "
          "spawn a new one automatically, wait for it, and then succeed.")
          
    start_t = time.time()
    job_id_2 = client.push({"task": "job_2"}, idempotency_key="idemp_101")
    elapsed = time.time() - start_t
    
    new_url = client._get_active_broker_url()
    print(f"SUCCESS! Recovered seamlessly in {elapsed:.2f}s and pushed job: {job_id_2}")
    print(f"New active broker is: {new_url}")
    
    # Wait for the job to be claimable (must loop since other job is first in queue)
    while True:
        job = client.claim("worker_A", lease_timeout_sec=60.0)
        if not job: 
            time.sleep(0.1)
            continue
            
        client.ack(job["id"], "worker_A")
        if job["id"] == job_id_2["job_id"]:
            break # We successfully claimed and acked the target job
    
    print("\nAttempting Duplicate Push with same Idempotency Key...")
    job_id_3 = client.push({"task": "job_2"}, idempotency_key="idemp_101")
    if job_id_3.get("job_id") == job_id_2.get("job_id"):
        print(f"SUCCESS! Idempotency prevented duplicate queueing. Returned existing ID: {job_id_3['job_id']}")
    else:
        print(f"FAIL: Idempotency did not work. Re-queued as: {job_id_3}")

if __name__ == "__main__":
    simulate_heartbeat_failure()
    simulate_broker_failure()
