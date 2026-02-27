import os
from src.queue.service import QueueClient

def main():
    # Clean up old queue files
    for ext in ['json', 'meta', 'lock']:
        try:
            os.remove(f"queue.{ext}")
        except FileNotFoundError:
            pass

    client = QueueClient("queue")
    print("Pushing jobs to queue...")
    client.push({"task": "render_video", "video_id": 101})
    client.push({"task": "send_email", "user_id": 42})
    client.push({"task": "resize_image", "image_id": 99})
    print(client.cas.read()[0])

    print("\nWorker A and B claiming jobs...")
    job1 = client.claim("worker_A")
    job2 = client.claim("worker_B")
    print(f"Worker A claimed: {job1['payload']['task']}")
    print(f"Worker B claimed: {job2['payload']['task']}")
    print(client.cas.read()[0])

    print("\nWorker B fails its job...")
    client.fail(job2['id'], "worker_B", max_attempts=3)
    print("Worker B failed out.")
    print(client.cas.read()[0])

    print("\nWorker A acks its job...")
    client.ack(job1['id'], "worker_A")
    print("Worker A acknowledged success.")
    print(client.cas.read()[0])
    
    print("\nCompacting the queue...")
    removed = client.compact()
    print(f"Removed {removed} jobs.")
    print(client.cas.read()[0])

if __name__ == "__main__":
    main()
