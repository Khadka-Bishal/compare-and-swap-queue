import asyncio
import os
import sys
import argparse
import logging
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from contextlib import asynccontextmanager
from typing import Optional, Dict, Any

from queue_ops import QueueClient
from buffered_client import BufferedQueueClient

logging.basicConfig(level=logging.INFO, format='[%(process)d] %(message)s')
logger = logging.getLogger("broker")

buffered_client: BufferedQueueClient
broker_url: str

async def broker_suicide_loop():
    """Continuously monitors queue.json to see if another broker took over."""
    while True:
        try:
            data, _ = buffered_client.queue_client.cas.read()
            current_broker = data.get("broker")
            if current_broker and current_broker != broker_url:
                logger.warning(f"FATAL: Superseded by new broker {current_broker}. Exiting...")
                os._exit(1) # Immediate hard exit to simulate crash/failover
        except Exception as e:
            logger.error(f"Error in suicide loop: {e}")
            
        await asyncio.sleep(2.0)

@asynccontextmanager
async def lifespan(app: FastAPI):
    global buffered_client
    
    base_client = QueueClient("queue")
    
    buffered_client = BufferedQueueClient(
        base_client, 
        max_batch_size=100, 
        flush_interval_ms=50
    )
    
    # Check if a specific URL was requested
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8000)
    args, _ = parser.parse_known_args()
    
    global broker_url
    broker_url = f"http://127.0.0.1:{args.port}"
    logger.info({"event": "broker_startup", "url": broker_url})
    
    # CAS-write this broker's URL as the active one
    buffered_client.register_broker(broker_url)
    
    # Start the suicide loop
    task = asyncio.create_task(broker_suicide_loop())
    
    yield
    
    task.cancel()
    buffered_client.shutdown()
    logger.info({"event": "broker_shutdown"})

app = FastAPI(lifespan=lifespan)

# --- Models ---
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


# --- Endpoints ---
@app.post("/push")
async def push_job(req: PushRequest):
    try:
        job_id = buffered_client.push(req.payload, req.idempotency_key)
        return {"status": "ok", "job_id": job_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/claim")
async def claim_job(req: ClaimRequest):
    try:
        job = buffered_client.claim(req.worker_id, lease_timeout_sec=req.lease_timeout_sec)
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
