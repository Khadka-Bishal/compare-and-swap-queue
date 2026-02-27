import time
import json
import logging
import threading
from typing import Any, Dict, List, Optional
from src.queue.storage import LocalCASObject, ConflictError
from src.queue.models import (
    Intent,
    RegisterBrokerIntent,
    PushIntent,
    ClaimIntent,
    AckIntent,
    FailIntent,
    HeartbeatIntent,
)

logger = logging.getLogger(__name__)

class QueueClient:
    def __init__(self, filename_base="queue"):
        self.cas = LocalCASObject(filename_base)
        # Ensure the queue has the right basic structure on init
        def init_queue(data):
            if "jobs" not in data:
                data["jobs"] = []
            if "broker" not in data:
                data["broker"] = None
            if "recently_done" not in data:
                data["recently_done"] = {}
            return data
            
        try:
            self.cas.update_with_retry(init_queue)
        except ConflictError:
            pass # Someone else won, that's fine
            
    def compact(self, archive_file="archive.jsonl"):
        """
        Removes 'done'/'dead' jobs from queue.json to keep it small.
        Appends them to an archive file.
        """
        archived_jobs = []
        
        def do_compact(data):
            nonlocal archived_jobs
            active = []
            archived_jobs.clear()
            
            for job in data.get("jobs", []):
                if job["state"] in ("done", "dead"):
                    archived_jobs.append(job)
                else:
                    active.append(job)
            
            data["jobs"] = active
            return data
            
        self.cas.update_with_retry(do_compact)
        
        if archived_jobs:
            with open(archive_file, "a") as f:
                for job in archived_jobs:
                    f.write(json.dumps(job) + "\n")
            logger.info(f"Compacted {len(archived_jobs)} jobs to {archive_file}")


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
                pass 
                
        # Block this caller's thread until the background thread processes the batch
        event.wait()
        
        if "error" in result_container:
            raise result_container["error"]
        return result_container.get("value")

    def register_broker(self, url: str):
        return self._submit(RegisterBrokerIntent(url))

    def push(self, payload: dict, idempotency_key: str = None):
        return self._submit(PushIntent(payload, idempotency_key))

    def claim(self, worker_id: str, lease_timeout_sec: float = 60.0):
        return self._submit(ClaimIntent(worker_id, lease_timeout_sec))

    def ack(self, job_id: str, worker_id: str):
        return self._submit(AckIntent(job_id, worker_id))

    def fail(self, job_id: str, worker_id: str):
        return self._submit(FailIntent(job_id, worker_id))

    def heartbeat(self, job_id: str, worker_id: str):
        return self._submit(HeartbeatIntent(job_id, worker_id))

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
        start_t = time.perf_counter()
        self.metrics["total_flushes"] += 1
        self.metrics["total_intents"] += len(batch)
        
        def apply_batch(data):
            for item in batch:
                intent = item["intent"]
                item["result"]["temp_value"] = intent.apply(data)
            return data

        try:
            self.queue_client.cas.update_with_retry(apply_batch)
            for item in batch:
                item["result"]["value"] = item["result"].get("temp_value")
                
        except ConflictError as e:
            self.metrics["total_conflicts"] += 1
            for item in batch:
                item["result"]["error"] = e
        except Exception as e:
            for item in batch:
                item["result"]["error"] = e
        finally:
            elapsed_ms = (time.perf_counter() - start_t) * 1000
            logger.info(json.dumps({
                "event": "flush_batch", 
                "batch_size": len(batch), 
                "duration_ms": round(elapsed_ms, 2)
            }))
            
            # Wake up all waiting callers
            for item in batch:
                item["event"].set()

    def shutdown(self):
        """Stops the background thread and flushes remaining intents."""
        self._stop_event.set()
        self._thread.join()
        
        with self._lock:
            while self._buffer:
                batch = self._buffer[:self.max_batch_size]
                self._buffer = self._buffer[self.max_batch_size:]
                self._process_batch(batch)
