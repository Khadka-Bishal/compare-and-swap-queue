import asyncio
import os
import argparse
import logging
from fastapi import FastAPI
from contextlib import asynccontextmanager

from src.config import settings
from src.queue.service import QueueClient, BufferedQueueClient
from src.queue.dependencies import set_queue_client
from src.queue.router import router as queue_router

logging.basicConfig(level=logging.INFO, format='[%(process)d] %(message)s')
logger = logging.getLogger("broker")

async def broker_suicide_loop(client: BufferedQueueClient, broker_url: str):
    """Continuously monitors queue.json to see if another broker took over."""
    while True:
        try:
            data, _ = client.queue_client.cas.read()
            current_broker = data.get("broker")
            if current_broker and current_broker != broker_url:
                logger.warning(f"FATAL: Superseded by new broker {current_broker}. Exiting...")
                os._exit(1) # Immediate hard exit to simulate crash/failover
        except Exception as e:
            logger.error(f"Error in suicide loop: {e}")
            
        await asyncio.sleep(2.0)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Parse port to support dynamic spinups
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=settings.port)
    args, _ = parser.parse_known_args()
    
    broker_url = f"http://127.0.0.1:{args.port}"
    
    base_client = QueueClient(settings.queue_storage_filename)
    buffered_client = BufferedQueueClient(
        base_client, 
        max_batch_size=100, 
        flush_interval_ms=50
    )
    
    # Inject dependency
    set_queue_client(buffered_client)
    
    logger.info({"event": "broker_startup", "url": broker_url})
    
    # CAS-write this broker's URL as the active one
    buffered_client.register_broker(broker_url)
    
    # Start the suicide loop
    task = asyncio.create_task(broker_suicide_loop(buffered_client, broker_url))
    
    yield
    
    task.cancel()
    buffered_client.shutdown()
    logger.info({"event": "broker_shutdown"})

app = FastAPI(lifespan=lifespan, title="JSON Queue Broker")
app.include_router(queue_router)
