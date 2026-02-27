import uuid
import time
from storage import LocalCASObject

class QueueClient:
    def __init__(self, filename_base="queue"):
        self.cas = LocalCASObject(filename_base)
        # Ensure the queue has the right basic structure on init
        def init_queue(data):
            if "jobs" not in data:
                data["jobs"] = []
            return data
        self.cas.update_with_retry(init_queue)

    def push(self, payload):
        """Adds a new job to the queue."""
        job_id = str(uuid.uuid4())
        new_job = {
            "id": job_id,
            "state": "queued",
            "payload": payload,
            "claimed_by": None,
            "heartbeat_ts": None,
            "created_ts": time.time(),
            "attempt": 0
        }

        def _push(data):
            # We append to the list of jobs
            data["jobs"].append(new_job)
            return data
            
        self.cas.update_with_retry(_push)
        return job_id

    def claim(self, worker_id):
        """
        Finds the oldest queued job, marks it in_progress, and claims it.
        Returns the job dict if claimed, or None if no jobs are available.
        """
        claimed_job = None
        
        def _claim(data):
            nonlocal claimed_job
            claimed_job = None # Reset on retry
            
            # Find the first available queued job (FIFO)
            for job in data.get("jobs", []):
                if job["state"] == "queued":
                    job["state"] = "in_progress"
                    job["claimed_by"] = worker_id
                    job["heartbeat_ts"] = time.time()
                    job["attempt"] += 1
                    claimed_job = job.copy()
                    break
                    
            return data

        self.cas.update_with_retry(_claim)
        return claimed_job

    def ack(self, job_id, worker_id):
        """Marks a job as done. Only the claiming worker should be able to ack."""
        success = False
        
        def _ack(data):
            nonlocal success
            success = False
            
            for job in data.get("jobs", []):
                if job["id"] == job_id:
                    if job["state"] == "in_progress" and job["claimed_by"] == worker_id:
                        job["state"] = "done"
                        job["claimed_by"] = None
                        success = True
                    break
            return data

        self.cas.update_with_retry(_ack)
        return success

    def fail(self, job_id, worker_id, max_attempts=3):
        """
        Returns a job to the queued state. 
        If attempts > max_attempts, marks it as dead/done.
        """
        success = False
        
        def _fail(data):
            nonlocal success
            success = False
            
            for job in data.get("jobs", []):
                if job["id"] == job_id:
                    if job["state"] == "in_progress" and job["claimed_by"] == worker_id:
                        if job["attempt"] >= max_attempts:
                            job["state"] = "dead" # Or "failed_permanently"
                        else:
                            job["state"] = "queued"
                            
                        job["claimed_by"] = None
                        job["heartbeat_ts"] = None
                        success = True
                    break
            return data

        self.cas.update_with_retry(_fail)
        return success

    def compact(self):
        """Removes all 'done' and 'dead' jobs to shrink the JSON file."""
        num_removed = 0
        
        def _compact(data):
            nonlocal num_removed
            original_len = len(data.get("jobs", []))
            
            # Keep only jobs that are NOT done and NOT dead
            data["jobs"] = [
                j for j in data.get("jobs", []) 
                if j["state"] not in ("done", "dead")
            ]
            
            num_removed = original_len - len(data["jobs"])
            return data

        self.cas.update_with_retry(_compact)
        return num_removed
