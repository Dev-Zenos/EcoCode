import docker
import time
import threading
import subprocess
import os
from pathlib import Path
import sys
import math
import json # Import the json module

# --- Constants NOT read from config (Could be moved to config too) ---
IMAGE_NAME = "python-sandbox:latest"
CONTAINER_NAME = "test_run_container" # Use unique names in a real app
# Docker Run Configuration (Could also be moved to config)
CPU_QUOTA = 50000   # e.g., 50000 for 0.5 cores
CPU_PERIOD = 100000 # e.g., 100000 (default)
MEM_LIMIT = '256m'  # Memory limit

# --- Dynamically Calculated ---
ALLOCATED_CPU_CORES = float(CPU_QUOTA) / float(CPU_PERIOD) if CPU_PERIOD > 0 else 0

# --- Globals for monitoring ---
stats_data = []
stop_monitoring = threading.Event()

# --- Helper Functions (parse_mem_string, monitor_container - unchanged) ---
def parse_mem_string(mem_str):
    """Parses docker stats memory string (e.g., '10.5MiB') into MiB."""
    mem_str = mem_str.lower().strip()
    try:
        if 'kib' in mem_str:
            return float(mem_str.replace('kib', '')) / 1024.0
        elif 'mib' in mem_str:
            return float(mem_str.replace('mib', ''))
        elif 'gib' in mem_str:
            return float(mem_str.replace('gib', '')) * 1024.0
        elif 'b' in mem_str:
             return float(mem_str.replace('b','')) / (1024.0*1024.0)
        else:
            return 0.0
    except ValueError:
        return 0.0

def monitor_container(container_name_or_id):
    """Polls docker stats for a given container."""
    print(f"Starting monitoring for {container_name_or_id}...")
    while not stop_monitoring.is_set():
        try:
            result = subprocess.run(
                [
                    'docker', 'stats', '--no-stream',
                    '--format', '{{.CPUPerc}},{{.MemUsage}}',
                    container_name_or_id
                ],
                capture_output=True, text=True, check=True, encoding='utf-8'
            )
            stdout = result.stdout.strip()
            if stdout:
                cpu_str, mem_str = stdout.split(',')
                timestamp = time.time()
                mem_usage_mib = parse_mem_string(mem_str.split('/')[0])
                stats_data.append({
                    'time': timestamp,
                    'cpu_perc_str': cpu_str,
                    'mem_usage_mib': mem_usage_mib
                 })
            else:
                print("Monitor: No stats output, container might have stopped.")
                break
        except subprocess.CalledProcessError as e:
            print(f"Monitor: 'docker stats' command failed (container likely stopped): {e.stderr}")
            break
        except FileNotFoundError:
            print("Monitor Error: 'docker' command not found. Is Docker installed and in PATH?")
            stop_monitoring.set()
            break
        except Exception as e:
            print(f"Monitor: Error getting stats: {e}")
            time.sleep(0.5)
        time.sleep(1)
    print(f"Stopping monitoring for {container_name_or_id}.")

# --- Function to load configuration ---
def load_config(config_path):
    """Loads configuration from a JSON file."""
    if not config_path.is_file():
        print(f"Error: Configuration file not found at {config_path}")
        sys.exit(1)
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        # Basic validation for required keys
        required_keys = ["user_code_dir_relative", "code_entrypoint", "power_assumptions"]
        for key in required_keys:
            if key not in config:
                print(f"Error: Missing required key '{key}' in configuration file.")
                sys.exit(1)
        required_power_keys = ["cpu_per_core_watt", "ram_per_gb_watt", "baseline_container_watt"]
        for key in required_power_keys:
             if key not in config["power_assumptions"]:
                 print(f"Error: Missing required key '{key}' under 'power_assumptions' in configuration file.")
                 sys.exit(1)
        return config
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from configuration file {config_path}: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred while loading config {config_path}: {e}")
        sys.exit(1)

# --- Main Execution ---
def run():
    SCRIPT_DIR = Path(__file__).parent.resolve()
    CONFIG_FILE_PATH = SCRIPT_DIR / "config.json"

    # --- Load Configuration ---
    print(f"Loading configuration from: {CONFIG_FILE_PATH}")
    config = load_config(CONFIG_FILE_PATH)

    # --- Get values from config ---
    try:
        USER_CODE_DIR_RELATIVE = config['user_code_dir_relative']
        CODE_ENTRYPOINT = config['code_entrypoint']
        POWER_CPU_PER_CORE_WATT = float(config['power_assumptions']['cpu_per_core_watt'])
        POWER_RAM_PER_GB_WATT = float(config['power_assumptions']['ram_per_gb_watt'])
        POWER_BASELINE_CONTAINER_WATT = float(config['power_assumptions']['baseline_container_watt'])
        POWER_NOTES = config['power_assumptions'].get('notes', '') # Optional notes field
    except KeyError as e:
         print(f"Error: Missing expected key in config data: {e}")
         sys.exit(1)
    except ValueError as e:
         print(f"Error: Could not convert power assumption value to float: {e}")
         sys.exit(1)

    # Resolve the user code directory path
    USER_CODE_DIR = (SCRIPT_DIR / USER_CODE_DIR_RELATIVE).resolve()
    # --- End Load Configuration ---

    # Check if user code directory exists
    if not USER_CODE_DIR.is_dir():
        print(f"Error: User code directory not found at resolved path: {USER_CODE_DIR}")
        print(f"(Based on relative path '{USER_CODE_DIR_RELATIVE}' in config)")
        sys.exit(1)

    print(f"Using user code from: {USER_CODE_DIR}")
    print(f"Entrypoint script: {CODE_ENTRYPOINT}")
    print(f"Allocated CPU cores for container: {ALLOCATED_CPU_CORES}")

    client = None
    container = None
    monitor_thread = None
    run_duration = 0
    start_run_time = 0

    try:
        # Connect to Docker daemon (same as before)
        try:
            client = docker.from_env()
            client.ping()
            print("Docker daemon connected.")
        except Exception as e:
            print(f"Error connecting to Docker daemon: {e}")
            sys.exit(1)

        # Build image if needed (same as before)
        try:
            client.images.get(IMAGE_NAME)
            print(f"Docker image '{IMAGE_NAME}' found.")
        except docker.errors.ImageNotFound:
            print(f"Docker image '{IMAGE_NAME}' not found. Building...")
            try:
                dockerfile_path = SCRIPT_DIR / "python-sandbox.Dockerfile"
                image, build_log = client.images.build(
                    path=str(SCRIPT_DIR),
                    dockerfile=str(dockerfile_path.name),
                    tag=IMAGE_NAME,
                    rm=True
                )
                print(f"Successfully built image '{IMAGE_NAME}'")
            except docker.errors.BuildError as e:
                print(f"Error building Docker image: {e}")
                sys.exit(1)
            except FileNotFoundError:
                 print(f"Error: Dockerfile not found at {dockerfile_path}")
                 sys.exit(1)

        # Run the container (using configured CODE_ENTRYPOINT)
        print(f"\nStarting container '{CONTAINER_NAME}'...")
        start_run_time = time.time()
        container = client.containers.run(
            image=IMAGE_NAME,
            command=["python", CODE_ENTRYPOINT], # Use config value
            name=CONTAINER_NAME,
            detach=True,
            user='appuser',
            network_disabled=True,
            mem_limit=MEM_LIMIT, # Use constant defined above
            memswap_limit=MEM_LIMIT, # Match mem_limit
            cpu_period=CPU_PERIOD,
            cpu_quota=CPU_QUOTA,
            volumes={str(USER_CODE_DIR): {'bind': '/app', 'mode': 'ro'}}, # Use resolved config path
            working_dir='/app',
            stdout=True,
            stderr=True
        )
        print(f"Container '{container.name}' ({container.short_id}) started.")

        # Start monitoring (same as before)
        stop_monitoring.clear()
        stats_data.clear()
        monitor_thread = threading.Thread(target=monitor_container, args=(container.name,))
        monitor_thread.start()

        # Wait for completion (same as before)
        print("Waiting for container to finish...")
        result = container.wait(timeout=120) # Increased timeout slightly
        exit_code = result.get('StatusCode', -1)
        end_run_time = time.time()
        run_duration = end_run_time - start_run_time
        print(f"Container finished with exit code: {exit_code} in {run_duration:.2f} seconds.")

        # Stop monitoring (same as before)
        print("Stopping monitor thread...")
        stop_monitoring.set()
        monitor_thread.join(timeout=5)
        if monitor_thread.is_alive():
             print("Warning: Monitor thread did not stop gracefully.")

        # Get logs (same as before)
        print("\n--- Container Logs ---")
        try:
            logs = container.logs().decode('utf-8', errors='replace')
            print(logs if logs else "[No output]")
        except Exception as e:
            print(f"Error retrieving logs: {e}")
        print("--- End Logs ---")


        # Process statistics and Calculate Energy (using configured power assumptions)
        print("\n--- Collected Stats & Energy Estimation ---")
        avg_cpu_perc = 0.0
        avg_mem_mib = 0.0
        peak_mem_mib = 0.0
        energy_kwh = 0.0 # Initialize energy to 0
        total_avg_power_watt = 0.0 # Initialize power

        if stats_data and run_duration > 0:
            total_cpu_perc_val = 0.0
            total_mem_mib = 0.0
            count = 0

            for stat in stats_data:
                try:
                    cpu_val = float(stat['cpu_perc_str'].replace('%', ''))
                    if not math.isnan(cpu_val):
                         total_cpu_perc_val += cpu_val
                    else:
                         print(f"Warning: NaN value encountered for CPU percentage: {stat['cpu_perc_str']}")

                    mem_val_mib = stat['mem_usage_mib']
                    if not math.isnan(mem_val_mib):
                        total_mem_mib += mem_val_mib
                        if mem_val_mib > peak_mem_mib:
                            peak_mem_mib = mem_val_mib
                    else:
                         print(f"Warning: NaN value encountered for Memory usage: {stat['mem_usage_mib']}")
                    count += 1
                except ValueError as e:
                    print(f"Warning: Could not parse stats entry: {stat} - Error: {e}")
                    continue

            if count > 0:
                avg_cpu_perc = total_cpu_perc_val / count
                avg_mem_mib = total_mem_mib / count
                avg_mem_gb = avg_mem_mib / 1024.0

                # Use config values for power calculation
                avg_cpu_power_watt = (avg_cpu_perc / 100.0) * ALLOCATED_CPU_CORES * POWER_CPU_PER_CORE_WATT
                avg_ram_power_watt = avg_mem_gb * POWER_RAM_PER_GB_WATT
                total_avg_power_watt = avg_cpu_power_watt + avg_ram_power_watt + POWER_BASELINE_CONTAINER_WATT

                energy_kwh = (total_avg_power_watt / 1000.0) * (run_duration / 3600.0)

            print(f"Number of Stat Samples: {len(stats_data)}")
            print(f"Total Runtime:          {run_duration:.2f} seconds")
            print(f"Average CPU Usage:      {avg_cpu_perc:.2f}%")
            print(f"Average Memory Usage:   {avg_mem_mib:.2f} MiB")
            print(f"Peak Memory Usage:      {peak_mem_mib:.2f} MiB")
            print("---")
            print(f"Estimated Average Power:{total_avg_power_watt:.3f} Watts")
            print(f"ESTIMATED ENERGY USAGE: {energy_kwh:.9f} kWh")
            print("\nNOTE: Energy is a ROUGH ESTIMATE based on configured assumptions:")
            # Print config values used
            print(f"  - Config Notes:       {POWER_NOTES}" if POWER_NOTES else "  - (No power notes in config)")
            print(f"  - CPU Power per Core: {POWER_CPU_PER_CORE_WATT} W")
            print(f"  - RAM Power per GB:   {POWER_RAM_PER_GB_WATT} W")
            print(f"  - Baseline Container: {POWER_BASELINE_CONTAINER_WATT} W")
            print(f"  - Allocated Cores:    {ALLOCATED_CPU_CORES}") # This is still calculated from constants

        else:
            print("No statistics collected or run duration too short to calculate energy.")

        print("--- End Stats & Energy Estimation ---")

    # Exception Handling (same as before)
    except docker.errors.NotFound as e:
         print(f"Docker Error: Resource not found - {e}")
    except docker.errors.APIError as e:
         print(f"Docker API Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()

    # Cleanup (same as before)
    finally:
        if container:
            try:
                print(f"\nCleaning up container '{CONTAINER_NAME}'...")
                if monitor_thread and monitor_thread.is_alive():
                     print("Waiting for monitor thread before cleanup...")
                     stop_monitoring.set()
                     monitor_thread.join(timeout=5)
                container.reload()
                if container.status == 'running':
                     print("Stopping container...")
                     container.stop(timeout=10)
                print("Removing container...")
                container.remove(v=True)
                print("Cleanup complete.")
            except docker.errors.NotFound:
                print("Container already removed or not found during cleanup.")
            except Exception as e:
                print(f"Error during cleanup: {e}")
        else:
             try:
                 if client:
                     cont = client.containers.get(CONTAINER_NAME)
                     print(f"Found leftover container '{CONTAINER_NAME}'. Attempting cleanup...")
                     cont.stop(timeout=10)
                     cont.remove(v=True)
                     print("Leftover container cleaned up.")
             except docker.errors.NotFound:
                 pass
             except AttributeError:
                 pass
             except Exception as e:
                 print(f"Error cleaning up potential leftover container by name: {e}")