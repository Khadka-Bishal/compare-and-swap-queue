import uuid
import time

class Intent:
    def apply(self, data: dict) -> any:
        raise NotImplementedError

class RegisterBrokerIntent(Intent):
    def __init__(self, broker_url: str):
        self.broker_url = broker_url
        
    def apply(self, data: dict):
        data["broker"] = self.broker_url
        return True

class PushIntent(Intent):
    def __init__(self, payload: dict, idempotency_key: str = None):
        self.payload = payload
        self.idempotency_key = idempotency_key
        
    def apply(self, data: dict):
        if self.idempotency_key and self.idempotency_key in data.get("recently_done", {}):
            return data["recently_done"][self.idempotency_key]
            
        job_id = str(uuid.uuid4())
        new_job = {
            "id": job_id,
            "state": "queued",
            "payload": self.payload,
            "claimed_by": None,
            "heartbeat_ts": None,
            "created_ts": time.time(),
            "attempt": 0,
            "idempotency_key": self.idempotency_key
        }
        if "jobs" not in data:
            data["jobs"] = []
        data["jobs"].append(new_job)
        return job_id

class ClaimIntent(Intent):
    def __init__(self, worker_id: str, lease_timeout_sec: float = 60.0):
        self.worker_id = worker_id
        self.lease_timeout_sec = lease_timeout_sec
        
    def apply(self, data: dict):
        now = time.time()
        
        for job in data.get("jobs", []):
            if job["state"] == "queued":
                job["state"] = "in_progress"
                job["claimed_by"] = self.worker_id
                job["heartbeat_ts"] = now
                job["attempt"] += 1
                return job.copy()
                
        for job in data.get("jobs", []):
            if job["state"] == "in_progress":
                if job["heartbeat_ts"] and (now - job["heartbeat_ts"] > self.lease_timeout_sec):
                    job["claimed_by"] = self.worker_id
                    job["heartbeat_ts"] = now
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
                    
                    i_key = job.get("idempotency_key")
                    if i_key:
                        if "recently_done" not in data:
                            data["recently_done"] = {}
                        data["recently_done"][i_key] = job["id"]
                        
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

class HeartbeatIntent(Intent):
    def __init__(self, job_id: str, worker_id: str):
        self.job_id = job_id
        self.worker_id = worker_id
        
    def apply(self, data: dict):
        for job in data.get("jobs", []):
            if job["id"] == self.job_id:
                if job["state"] == "in_progress" and job["claimed_by"] == self.worker_id:
                    job["heartbeat_ts"] = time.time()
                    return True
                break
        return False
