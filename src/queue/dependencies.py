from src.queue.service import BufferedQueueClient

# Global reference populated during app lifespan
_buffered_client_instance: BufferedQueueClient = None

def get_queue_client() -> BufferedQueueClient:
    """FastAPI Dependency for accessing the Queue Broker."""
    if not _buffered_client_instance:
        raise RuntimeError("Buffered queue client is not initialized.")
    return _buffered_client_instance

def set_queue_client(client: BufferedQueueClient):
    global _buffered_client_instance
    _buffered_client_instance = client
