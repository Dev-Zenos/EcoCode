import time
import math
import random
import string
import os
import hashlib
import gc  # Garbage Collector interface

def generate_random_string(length):
    """Generates a random string of fixed length."""
    letters = string.ascii_lowercase + string.digits
    return ''.join(random.choice(letters) for i in range(length))

def cpu_intensive_task(duration_sec):
    """Performs CPU-bound calculations for a given duration."""
    print(f"--- Starting CPU-intensive task ({duration_sec}s) ---")
    start_time = time.time()
    count = 0
    result = 0
    while time.time() - start_time < duration_sec:
        # More complex calculation involving hashing and math
        data_str = generate_random_string(50)
        hash_obj = hashlib.sha256(data_str.encode('utf-8')).hexdigest()
        result += math.log(int(hash_obj[:8], 16) + 1) # Use part of hash
        count += 1
        # Optional tiny sleep to prevent 100% pegging on some systems,
        # but keep it CPU-bound mostly. Remove if you want max CPU.
        # time.sleep(0.0001)
    print(f"--- CPU task finished. Iterations: {count} ---")
    return result

def memory_allocation_task(target_mib):
    """Allocates memory by building a list of strings."""
    print(f"--- Starting Memory Allocation task (target: {target_mib} MiB) ---")
    large_data_list = []
    current_size_bytes = 0
    target_size_bytes = target_mib * 1024 * 1024
    string_length = 1024 # Allocate 1KB strings

    while current_size_bytes < target_size_bytes:
        new_string = generate_random_string(string_length)
        large_data_list.append(new_string)
        # Rough estimate of size increase (doesn't account for list overhead perfectly)
        current_size_bytes += len(new_string.encode('utf-8')) # More accurate size estimate
        if len(large_data_list) % 1000 == 0: # Print progress periodically
             print(f"  Allocated ~{current_size_bytes / (1024*1024):.1f} MiB...")

    allocated_mib = current_size_bytes / (1024*1024)
    print(f"--- Memory task finished. Allocated ~{allocated_mib:.2f} MiB in {len(large_data_list)} list items ---")
    return large_data_list

def mixed_workload_task(data_list, duration_sec):
    """Simulates work involving CPU bursts and I/O waits using existing data."""
    print(f"--- Starting Mixed Workload task ({duration_sec}s) ---")
    start_time = time.time()
    count = 0
    list_len = len(data_list)

    if list_len == 0:
        print("Warning: Data list is empty for mixed workload.")
        time.sleep(duration_sec) # Just wait if no data
        return

    while time.time() - start_time < duration_sec:
        # 1. CPU Burst: Process a few items from the list
        items_to_process = random.randint(5, 20)
        for _ in range(items_to_process):
            index = random.randint(0, list_len - 1)
            item = data_list[index]
            # Simulate processing: hash it again or do other computation
            _ = hashlib.sha1(item.encode('utf-8')).hexdigest()
            count += 1

        # 2. Simulate I/O Wait (e.g., waiting for network or disk)
        wait_time = random.uniform(0.05, 0.2) # Wait 50-200ms
        # print(f"  Simulating I/O wait: {wait_time:.3f}s") # Uncomment for verbose wait times
        time.sleep(wait_time)

    print(f"--- Mixed workload finished. Processed items (approx): {count} ---")


# --- Main Script Execution ---
if __name__ == "__main__":
    total_runtime_target = 25 # Target total seconds for the script

    start_script_time = time.time()
    print(f"Complex sample code started. Target runtime: ~{total_runtime_target}s. PID: {os.getpid()}, UID: {os.geteuid()}")

    # --- Phase 1: Initial CPU Burst ---
    phase1_duration = 5
    cpu_intensive_task(phase1_duration)

    # --- Phase 2: Memory Allocation ---
    target_mem = 50 # Allocate ~50 MiB
    allocated_data = memory_allocation_task(target_mem)

    # --- Phase 3: Mixed Workload (CPU + Simulated I/O) ---
    phase3_duration = 12
    mixed_workload_task(allocated_data, phase3_duration)

    # --- Phase 4: Memory Release & Final CPU Burst ---
    print(f"--- Releasing allocated memory ({len(allocated_data)} items) ---")
    del allocated_data # Hint to Python to release memory
    gc.collect() # Explicitly ask garbage collector to run (may or may not free immediately)
    print("--- Memory released (garbage collection requested) ---")
    time.sleep(1) # Short pause to allow monitoring to potentially see memory drop

    # Calculate remaining time for a final burst
    elapsed_time = time.time() - start_script_time
    phase4_duration = max(1, total_runtime_target - elapsed_time) # Ensure at least 1s
    cpu_intensive_task(phase4_duration)


    # --- End ---
    end_script_time = time.time()
    actual_duration = end_script_time - start_script_time
    print(f"\nComplex sample code finished. Total actual duration: {actual_duration:.2f} seconds.")