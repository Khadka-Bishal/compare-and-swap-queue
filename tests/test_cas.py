import multiprocessing
import os
import sys

# Import our class from storage.py
try:
    from src.queue.storage import LocalCASObject
except ImportError:
    print("Could not import LocalCASObject. Make sure storage.py is in the same directory.")
    sys.exit(1)

def worker(worker_id, num_increments):
    # Each worker gets its own instance pointing to the same base filename
    cas_obj = LocalCASObject("queue")
    
    for i in range(num_increments):
        def increment_counter(data):
            # Data might be empty initially
            count = data.get("counter", 0)
            data["counter"] = count + 1
            return data
            
        try:
            cas_obj.update_with_retry(increment_counter, max_retries=20, base_delay=0.01)
        except Exception as e:
            print(f"Worker {worker_id} failed on increment {i}: {e}")

if __name__ == "__main__":
    NUM_WORKERS = 5
    NUM_INCREMENTS = 20
    
    # Clean up old files if they exist to start fresh
    for ext in ['json', 'meta', 'lock']:
        try:
            os.remove(f"queue.{ext}")
        except FileNotFoundError:
            pass

    print(f"Starting {NUM_WORKERS} workers, each incrementing {NUM_INCREMENTS} times.")
    
    #    Spawn multiple processes to hammer the CAS object
    processes = []
    for i in range(NUM_WORKERS):
        p = multiprocessing.Process(target=worker, args=(i, NUM_INCREMENTS))
        processes.append(p)
        p.start()
        
    for p in processes:
        p.join()
        
    #       Verify the final state
    cas_obj = LocalCASObject("queue")
    final_data, final_version_num = cas_obj.read()
    
    expected_count = NUM_WORKERS * NUM_INCREMENTS
    actual_count = final_data.get("counter", 0)
    
    print("-" * 40)
    print(f"Expected final count: {expected_count}")
    print(f"Actual final count:   {actual_count}")
    print(f"Final version number: {final_version_num}")
    
    if expected_count == actual_count:
        print("SUCCESS! CAS logic worked perfectly. No lost updates.")
    else:
        print("FAILED! Lost updates detected.")
