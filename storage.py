import os
import json
import fcntl
import logging
import time
import random

logging.basicConfig(level=logging.INFO, format='[%(process)d] %(message)s')
logger = logging.getLogger(__name__)

class ConflictError(Exception):
    pass

class LocalCASObject:
    """
    A local concurrency-safe wrapper for a versioned JSON object.
    Emulates optimistic concurrency control found in remote object stores
    using a dedicated lock file and atomic replaces.
    """
    def __init__(self, filename_base):
        self.data_file = f"{filename_base}.json"
        self.meta_file = f"{filename_base}.meta"
        self.lock_file = f"{filename_base}.lock"
        
        # Initialize if they don't exist
        if not os.path.exists(self.meta_file):
            self._write_meta(0)
        if not os.path.exists(self.data_file):
            self._write_data({})

    def _write_meta(self, version):
        tmp_meta = f"{self.meta_file}.tmp"
        with open(tmp_meta, 'w') as f:
            json.dump({"version": version}, f)
        os.replace(tmp_meta, self.meta_file)

    def _write_data(self, data):
        tmp_data = f"{self.data_file}.tmp"
        with open(tmp_data, 'w') as f:
            json.dump(data, f)
        os.replace(tmp_data, self.data_file)

    def read(self):
        """
        Read the current data and version.
        Returns:
            (dict, int): The JSON data and the current version.
        """
        # Obtain a shared lock to ensure we don't read while a write is 
        # happening, even though atomic os.replace makes this largely safe.
        with open(self.lock_file, 'a') as lockf:
            fcntl.flock(lockf.fileno(), fcntl.LOCK_SH)
            try:
                try:
                    with open(self.meta_file, 'r') as f:
                        meta = json.load(f)
                        version = meta.get("version", 0)
                except (FileNotFoundError, json.JSONDecodeError):
                    version = 0

                try:
                    with open(self.data_file, 'r') as f:
                        data = json.load(f)
                except (FileNotFoundError, json.JSONDecodeError):
                    data = {}
            finally:
                fcntl.flock(lockf.fileno(), fcntl.LOCK_UN)
        
        return data, version

    def cas_write(self, new_data, expected_version):
        """
        Attempts to write new_data only if the current version matches expected_version.
        Returns:
            (True, new_version) if successful.
            (False, current_version) if a conflict occurred.
        """
        # Open lock file to synchronize across POSIX processes
        with open(self.lock_file, 'a') as lockf:
            # Block until we get exclusive access
            fcntl.flock(lockf.fileno(), fcntl.LOCK_EX)
            try:
                # Read current version under lock
                try:
                    with open(self.meta_file, 'r') as f:
                        meta = json.load(f)
                        current_version = meta.get("version", 0)
                except (FileNotFoundError, json.JSONDecodeError):
                    current_version = 0

                if current_version != expected_version:
                    return False, current_version

                new_version = current_version + 1
                
                # Write data atomically
                self._write_data(new_data)
                # Write meta atomically
                self._write_meta(new_version)
                
                return True, new_version
            finally: # Ensure lock is always released
                fcntl.flock(lockf.fileno(), fcntl.LOCK_UN)

    def update_with_retry(self, update_fn, max_retries=10, base_delay=0.01):
        """
        Helper method to apply an update function with CAS retries,
        exponential backoff, and jitter.
        
        update_fn: A callable that accepts a dict and returns a modified dict.
        """
        for attempt in range(max_retries):
            data, version = self.read()
            
            # Apply transformation
            new_data = update_fn(data)
            
            # Attempt CAS write
            ok, new_version = self.cas_write(new_data, version)
            if ok:
                logger.info(f"Successfully updated to version {new_version} on attempt {attempt + 1}")
                return new_data, new_version
            
            # If conflict, calculate delay with exponential backoff & jitter
            delay = base_delay * (2 ** attempt) + random.uniform(0, 0.05)
            logger.warning(f"Conflict on version {version}. Retrying in {delay:.3f}s (Attempt {attempt + 1}/{max_retries})")
            time.sleep(delay)
            
        raise ConflictError(f"Failed to update after {max_retries} attempts.")
