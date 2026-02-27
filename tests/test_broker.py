import os
import subprocess
import time
import httpx

def main():
    # Clean up old queue files
    for ext in ['json', 'meta', 'lock']:
        try:
            os.remove(f"queue.{ext}")
        except FileNotFoundError:
            pass

    print("Starting FastAPI Broker...")
    server = subprocess.Popen(
        [".venv/bin/uvicorn", "src.main:app", "--host", "127.0.0.1", "--port", "8000"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    
    # Wait for server to boot
    time.sleep(2)
    
    try:
        url = "http://127.0.0.1:8000"
        
        # Push jobs
        print("\n--- Pushing jobs ---")
        for i in range(5):
            resp = httpx.post(f"{url}/push", json={"payload": {"task": f"job_{i}"}})
            print("Push resp:", resp.json())
            
        #   Claim jobs
        print("\n--- Claiming jobs ---")
        job1 = httpx.post(f"{url}/claim", json={"worker_id": "worker_A"}).json()["job"]
        job2 = httpx.post(f"{url}/claim", json={"worker_id": "worker_B"}).json()["job"]
        print("Worker A claimed:", job1["payload"]["task"])
        print("Worker B claimed:", job2["payload"]["task"])
        
        # Heartbeat
        print("\n--- Heartbeat ---")
        resp = httpx.post(f"{url}/heartbeat", json={"job_id": job1["id"], "worker_id": "worker_A"})
        print("Heartbeat resp:", resp.json())
        
        # Ack
        print("\n--- Acking jobs ---")
        resp = httpx.post(f"{url}/ack", json={"job_id": job1["id"], "worker_id": "worker_A"})
        print("Ack resp:", resp.json())
        
        # Metrics
        print("\n--- Metrics ---")
        metrics = httpx.get(f"{url}/metrics").json()
        print(metrics)
        
    finally:
        print("\nShutting down server...")
        server.terminate()
        try:
            server.wait(timeout=3)
        except subprocess.TimeoutExpired:
            server.kill()
            
        # Print the server logs to see the structured flush logs
        print("\n--- Server Logs ---")
        print(server.stdout.read())

if __name__ == "__main__":
    main()
