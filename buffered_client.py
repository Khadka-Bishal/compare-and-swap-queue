import uuid
import time
import threading
import logging
from typing import Any, Dict, List, Optional
from queue_ops import QueueClient
from storage import ConflictError

logger = logging.getLogger(__name__)

# --- INTENTS ---
class Intent:
    def apply(self, data: dict) -> Any:
        raise NotImplementedError

class PushIntent(Intent):
    def __init__(self, payload: dict):
        self.payload = payload
        
    def apply(self, data: dict):
        job_id = str(uuid.uuid4())
        new_job = {
            "id": job_id,
            "state": "queued",
            "payload": self.payload,
            "claimed_by": None,
            "heartbeat_ts": None,
            "created_ts": time.time(),
            "attempt": 0
        }
        if "jobs" not in data:
            data["jobs"] = []
        data["jobs"].append(new_job)
        return job_id

class ClaimIntent(Intent):
    def __init__(self, worker_id: str):
        self.worker_id = worker_id
        
    def apply(self, data: dict):
        for job in data.get("jobs", []):
            if job["state"] == "queued":
                job["state"] = "in_progress"
                job["claimed_by"] = self.worker_id
                job["heartbeat_ts"] = time.time()
                job["attempt"] += 1
                return job.copy()
        return None

class AckIntent(Intent):
    def __init__(self, job_id: str, worker_id: str):
        self.job_id = job_id
        self.worker_id = worker_id
        
    def apply(self, data: dict):
        for job in data.get("jobs", []):
            if job["id"] == self.job_id:
                if job["state"] == "in_progress" and job["claimed_by"] == self.worker_id:
                    job["state"] = "done"
                    job["claimed_by"] = None
                    return True
                break
        return False

class FailIntent(Intent):
    def __init__(self, job_id: str, worker_id: str, max_attempts: int = 3):
        self.job_id = job_id
        self.worker_id = worker_id
        self.max_attempts = max_attempts
        
    def apply(self, data: dict):
        for job in data.get("jobs", []):
            if job["id"] == self.job_id:
                if job["state"] == "in_progress" and job["claimed_by"] == self.worker_id:
                    if job["attempt"] >= self.max_attempts:
                        job["state"] = "dead"
                    else:
                        job["state"] = "queued"
                    job["claimed_by"] = None
                    job["heartbeat_ts"] = None
                    return True
                break
        return False


# --- BUFFER MAPPER ---
class BufferedQueueClient:
    def __init__(self, queue_client: QueueClient, max_batch_size=100, flush_interval_ms=50):
        self.queue_client = queue_client
        self.max_batch_size = max_batch_size
        self.flush_interval_sec = flush_interval_ms / 1000.0
        
        # Buffer of (Intent, Event, ResultContainer)
        self._buffer: List[Dict] = []
        self._lock = threading.Lock()
        
        # Metrics
        self.metrics = {
            "total_flushes": 0,
            "total_intents": 0,
            "total_conflicts": 0,
        }
        
        # Start background flusher
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._flush_loop, daemon=True)
        self._thread.start()

    def _submit(self, intent: Intent) -> Any:
        """Submits an intent to the buffer and blocks until resolved."""
        event = threading.Event()
        result_container = {}
        
        with self._lock:
            self._buffer.append({
                "intent": intent,
                "event": event,
                "result": result_container
            })
            # If we hit max batch size, wake up the flush thread immediately
            if len(self._buffer) >= self.max_batch_size:
                pass # We could implement a Condition variable here for instant wake-up, 
                     # but interval polling is simpler for this local demo.
                
        # Block this caller's thread until the background thread processes the batch
        event.wait()
        
        if "error" in result_container:
            raise result_container["error"]
        return result_container.get("value")

    def push(self, payload: dict):
        return self._submit(PushIntent(payload))

    def claim(self, worker_id: str):
        return self._submit(ClaimIntent(worker_id))

    def ack(self, job_id: str, worker_id: str):
        return self._submit(AckIntent(job_id, worker_id))

    def fail(self, job_id: str, worker_id: str):
        return self._submit(FailIntent(job_id, worker_id))

    def _flush_loop(self):
        """Background thread that continuously flushes buffered intents."""
        while not self._stop_event.is_set():
            time.sleep(self.flush_interval_sec)
            
            with self._lock:
                if not self._buffer:
                    continue
                
                # Take up to max_batch_size items from the buffer
                batch = self._buffer[:self.max_batch_size]
                self._buffer = self._buffer[self.max_batch_size:]
                
            if batch:
                self._process_batch(batch)

    def _process_batch(self, batch: List[Dict]):
        """Runs the CAS loop to apply the whole batch of intents at once."""
        self.metrics["total_flushes"] += 1
        self.metrics["total_intents"] += len(batch)
        
        def apply_batch(data):
            # We apply each intent in sequence to the in-memory dictionary
            for item in batch:
                intent = item["intent"]
                item["result"]["temp_value"] = intent.apply(data)
            return data

        try:
            # Execute the batch as a single CAS transaction
            self.queue_client.cas.update_with_retry(apply_batch)
            
            # If CAS succeeded, lock in the temp results
            for item in batch:
                item["result"]["value"] = item["result"].get("temp_value")
                
        except ConflictError as e:
            # If it totally fails after 10-20 retries, bubble error to threads
            for item in batch:
                item["result"]["error"] = e
        except Exception as e:
            for item in batch:
                item["result"]["error"] = e
        finally:
            # Wake up all waiting callers
            for item in batch:
                item["event"].set()

    def shutdown(self):
        """Stops the background thread and flushes remaining intents."""
        self._stop_event.set()
        self._thread.join()
        
        # Flush whatever is left
        with self._lock:
            while self._buffer:
                batch = self._buffer[:self.max_batch_size]
                self._buffer = self._buffer[self.max_batch_size:]
                self._process_batch(batch)
