import logging
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from contextlib import asynccontextmanager
from typing import Optional, Dict, Any

from queue_ops import QueueClient
from buffered_client import BufferedQueueClient

# Setup basic logging to see our structured JSON logs
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger("broker")

# The global queue client instance
buffered_client: BufferedQueueClient

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Initialize the CAS wrapper and the buffered client
    global buffered_client
    base_client = QueueClient("queue")
    
    # Can configure max batch size and flush interval here
    buffered_client = BufferedQueueClient(
        base_client, 
        max_batch_size=100, 
        flush_interval_ms=50
    )
    logger.info({"event": "broker_startup", "message": "Buffered queue client initialized."})
    
    yield
    
    # Shutdown: Cleanly stop the background flush thread
    buffered_client.shutdown()
    logger.info({"event": "broker_shutdown", "message": "Buffered queue client stopped."})

app = FastAPI(lifespan=lifespan)

# --- Models ---
class PushRequest(BaseModel):
    payload: Dict[str, Any]

class ClaimRequest(BaseModel):
    worker_id: str

class AckRequest(BaseModel):
    job_id: str
    worker_id: str

class HeartbeatRequest(BaseModel):
    job_id: str
    worker_id: str


# --- Endpoints ---
@app.post("/push")
async def push_job(req: PushRequest):
    try:
        job_id = buffered_client.push(req.payload)
        return {"status": "ok", "job_id": job_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/claim")
async def claim_job(req: ClaimRequest):
    try:
        job = buffered_client.claim(req.worker_id)
        return {"status": "ok", "job": job}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/ack")
async def ack_job(req: AckRequest):
    try:
        success = buffered_client.ack(req.job_id, req.worker_id)
        if not success:
            raise HTTPException(status_code=400, detail="Could not ack job. Invalid state or ownership.")
        return {"status": "ok"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/heartbeat")
async def heartbeat_job(req: HeartbeatRequest):
    try:
        success = buffered_client.heartbeat(req.job_id, req.worker_id)
        if not success:
            raise HTTPException(status_code=400, detail="Could not heartbeat job. Invalid state or ownership.")
        return {"status": "ok"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/metrics")
async def get_metrics():
    return {
        "status": "ok",
        "metrics": buffered_client.metrics
    }
