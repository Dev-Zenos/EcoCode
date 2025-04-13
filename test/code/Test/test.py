import time
import threading
import multiprocessing
import random
import math

def cpu_intensive_task():
    """A function that performs CPU-intensive calculations."""
    start_time = time.time()
    while time.time() - start_time < 2:  # Run for 2 seconds per iteration
        # Perform some meaningless but CPU-intensive calculations
        for i in range(1000000):
            math.sqrt(random.random() * 10000)
            math.sin(random.random() * 360)
            math.cos(random.random() * 360)
            math.tan(random.random() * 360)

def run_load_test(duration=30):
    """Run a load test for the specified duration in seconds."""
    print(f"Starting load test for {duration} seconds...")
    
    # Determine the number of CPU cores
    num_cores = multiprocessing.cpu_count()
    print(f"Detected {num_cores} CPU cores")
    
    # Create threads for each core to maximize CPU usage
    start_time = time.time()
    end_time = start_time + duration
    
    while time.time() < end_time:
        threads = []
        print(f"Time remaining: {int(end_time - time.time())} seconds")
        
        # Create and start threads (one per CPU core)
        for i in range(num_cores):
            thread = threading.Thread(target=cpu_intensive_task)
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
    
    elapsed_time = time.time() - start_time
    print(f"Load test completed. Actual runtime: {elapsed_time:.2f} seconds")

if __name__ == "__main__":
    try:
        run_load_test(30)
    except KeyboardInterrupt:
        print("\nLoad test interrupted by user")