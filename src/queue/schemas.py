from typing import Dict, Any, Optional
from pydantic import BaseModel

class PushRequest(BaseModel):
    payload: Dict[str, Any]
    idempotency_key: Optional[str] = None

class ClaimRequest(BaseModel):
    worker_id: str
    lease_timeout_sec: float = 60.0

class AckRequest(BaseModel):
    job_id: str
    worker_id: str

class HeartbeatRequest(BaseModel):
    job_id: str
    worker_id: str
