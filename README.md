A lightweight, highly-available, distributed job queue built entirely on top of a single local JSON file using atomic Compare-And-Swap (CAS) operations. 

This project is a functional, local implementation of the architecture described in [Turbopuffer's "How to build a distributed queue in a single JSON file on object storage"](https://turbopuffer.com/blog/object-storage-queue).

When building multi-agent AI systems or high-throughput async processing pipelines, managing state is the hardest part. You usually need bulky infrastructure like Redis, Celery, or RabbitMQ.

This project proves you can achieve **robust distributed coordination** using nothing more than a dumb storage system (like AWS S3, Google Cloud Storage, or in this case, a local OS file) as long as it supports atomic CAS (Compare-And-Swap). 

- **Idempotent Pushes:** If an agent gets disconnected and pushes the same "Generate Image" job twice, the queue guarantees it only processes once.
- **Failover / High Availability:** If the active Broker process crashes mid-generation, a Smart Client will instantly detect the failure, spawn a brand new Broker, elect it via the JSON file, and seamlessly retry.
- **Lease Timeout:** If a worker node (or AI agent) crashes while holding a job, the lease expires and another worker automatically reclaims it.
- **Batched Throughput:** Instead of bottlenecking on disk I/O, the broker groups thousands of concurrent agent requests into a single atomic JSON write.

## How It Works

The system operates in 4 conceptual layers:

1. **Storage Layer (`src/queue/storage.py`)**: Uses temporary files and `os.rename` to provide atomic Compare-And-Swap over a single `queue.json` file.
2. **Buffer Layer (`src/queue/service.py`)**: Groups incoming requests (Push, Claim, Ack, Heartbeat) into "Intents" and flushes them to disk as a single transaction every 50ms.
3. **Broker API (`src/queue/router.py`)**: A structured FastAPI server that exposes HTTP endpoints to workers.
4. **Smart Client (`src/client/smart_client.py`)**: A self-healing HTTP client. It reads the raw `queue.json` to find the active broker's URL. If the URL is dead, it spawns a new broker automatically.

## How to Play with It

First, install the necessary dependencies:
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install fastapi httpx uvicorn pydantic pydantic-settings
```

### The pure CAS Benchmark
Watch 20 independent Python threads try to hammer the JSON file directly. You'll see thousands of conflicts as they fight for the single CAS lock.
```bash
PYTHONPATH=. python3 tests/test_benchmark.py
# (Wait for Phase 1 to finish, then watch Phase 4 seamlessly batch them)
```

### The Chaos / Fault Tolerance Simulation
Watch the queue recover from catastrophes in real-time.
```bash
PYTHONPATH=. python3 tests/test_faults.py
```
This script will:
1. Hard-crash a worker to prove lease timeouts work and jobs are reclaimed.
2. Hard-terminate the active FastAPI broker while it's processing data.
3. Prove that the SmartClient automatically detects the downtime, elects a new port, boots a new FastAPI instance, and successfully finishes the pending request *without* any data loss.

## Architecture

```mermaid
graph TD
    subgraph "Layer 4: Smart Client"
        W1[Worker 1]
        W2[Worker 2]
        P1[Pusher 1]
        SC[Smart Client Library]
        
        W1 --> SC
        W2 --> SC
        P1 --> SC
    end

    subgraph "Layer 3: Broker API (FastAPI)"
        API[HTTP Router Endpoints]
    end

    subgraph "Layer 2: Buffer Layer"
        Buffer[(In-Memory Intent Array)]
        Flusher((Background Flusher Loop))
    end

    subgraph "Layer 1: Storage Layer"
        CAS>LocalCASObject Engine]
        JSON[(queue.json)]
    end

    SC -->|HTTP POST| API
    
    API -->|Appends Intent| Buffer
    Flusher -->|Polls every 50ms| Buffer
    Flusher -->|Batch execution| CAS
    
    CAS -->|1. Reads| JSON
    CAS -->|2. Validates lock| JSON
    CAS -->|3. Overwrites atomic| JSON
```