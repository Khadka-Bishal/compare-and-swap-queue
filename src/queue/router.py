from fastapi import APIRouter, HTTPException, Depends
from src.queue.schemas import PushRequest, ClaimRequest, AckRequest, HeartbeatRequest
from src.queue.dependencies import get_queue_client
from src.queue.service import BufferedQueueClient

router = APIRouter(tags=["Queue"])

# CRITICAL FIX: These routes are `def` instead of `async def` 
# because `client.push/claim/ack()` use thread locks and block the thread.
# FastAPI will automatically run `def` routes in a background threadpool,
# preventing the main asyncio event loop from getting blocked.

@router.post("/push")
def push_job(req: PushRequest, client: BufferedQueueClient = Depends(get_queue_client)):
    try:
        job_id = client.push(req.payload, req.idempotency_key)
        return {"status": "ok", "job_id": job_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/claim")
def claim_job(req: ClaimRequest, client: BufferedQueueClient = Depends(get_queue_client)):
    try:
        job = client.claim(req.worker_id, lease_timeout_sec=req.lease_timeout_sec)
        return {"status": "ok", "job": job}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/ack")
def ack_job(req: AckRequest, client: BufferedQueueClient = Depends(get_queue_client)):
    try:
        success = client.ack(req.job_id, req.worker_id)
        if not success:
            raise HTTPException(status_code=400, detail="Could not ack job. Invalid state or ownership.")
        return {"status": "ok"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/heartbeat")
def heartbeat_job(req: HeartbeatRequest, client: BufferedQueueClient = Depends(get_queue_client)):
    try:
        success = client.heartbeat(req.job_id, req.worker_id)
        if not success:
            raise HTTPException(status_code=400, detail="Could not heartbeat job. Invalid state or ownership.")
        return {"status": "ok"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/metrics")
def get_metrics(client: BufferedQueueClient = Depends(get_queue_client)):
    return {
        "status": "ok",
        "metrics": client.metrics
    }
