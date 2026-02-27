import time
import httpx
import logging
import subprocess
import socket
from src.queue.service import QueueClient

logger = logging.getLogger(__name__)

def get_free_port():
    """Finds an available local port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))
        return s.getsockname()[1]

class SmartQueueClient:
    """
    Client wrapper that reads the queue file to find the active broker URL.
    If the broker is dead, it spawns a new broker automatically and retries.
    """
    def __init__(self, filename_base="queue"):
        self.queue_client = QueueClient(filename_base)
        self.http_client = httpx.Client(timeout=5.0)

    def _get_active_broker_url(self):
        data, _ = self.queue_client.cas.read()
        return data.get("broker")

    def _spawn_new_broker(self) -> str:
        port = get_free_port()
        new_url = f"http://127.0.0.1:{port}"
        logger.warning(f"Spawning new broker at {new_url}...")
        
        # We spawn the broker in the background
        subprocess.Popen(
            [".venv/bin/uvicorn", "src.main:app", "--host", "127.0.0.1", "--port", str(port)],
            stdout=subprocess.DEVNULL, # Keep terminal clean in this demo
            stderr=subprocess.DEVNULL
        )
        
        # Wait for it to boot and register
        for _ in range(20): # max 2 seconds
            time.sleep(0.1)
            data, _ = self.queue_client.cas.read()
            if data.get("broker") == new_url:
                logger.info(f"New broker {new_url} successfully took over.")
                return new_url
                
        raise Exception("Failed to boot new broker.")

    def _make_request(self, method: str, endpoint: str, json_data: dict = None, retry_count: int = 0):
        url = self._get_active_broker_url()
        
        if not url:
            url = self._spawn_new_broker()

        try:
            if method.upper() == "GET":
                resp = self.http_client.get(f"{url}{endpoint}")
            else:
                resp = self.http_client.post(f"{url}{endpoint}", json=json_data)
            resp.raise_for_status()
            return resp.json()
        except (httpx.ConnectError, httpx.RequestError) as e:
            if retry_count < 2:
                logger.warning(f"Broker at {url} seems dead. Initiating failover...")
                self._spawn_new_broker()
                return self._make_request(method, endpoint, json_data, retry_count + 1)
            else:
                raise Exception(f"Failed to communicate with broker after 3 retries: {e}")

    # --- Queue API ---
    def push(self, payload: dict, idempotency_key: str = None):
        return self._make_request("POST", "/push", {"payload": payload, "idempotency_key": idempotency_key})

    def claim(self, worker_id: str, lease_timeout_sec: float = 60.0):
        resp = self._make_request("POST", "/claim", {"worker_id": worker_id, "lease_timeout_sec": lease_timeout_sec})
        return resp.get("job")

    def ack(self, job_id: str, worker_id: str):
        return self._make_request("POST", "/ack", {"job_id": job_id, "worker_id": worker_id})

    def heartbeat(self, job_id: str, worker_id: str):
        return self._make_request("POST", "/heartbeat", {"job_id": job_id, "worker_id": worker_id})
