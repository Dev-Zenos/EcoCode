import docker
import time
import threading
import subprocess
import os
from pathlib import Path
import sys
import math
import json
import traceback
import requests # Import requests to catch its specific exceptions

# --- Constants ---
IMAGE_NAME = "python-sandbox:latest"
CONTAINER_NAME_PREFIX = "test_run_container_"
CPU_QUOTA = 50000
CPU_PERIOD = 100000
MEM_LIMIT = '256m'
ALLOCATED_CPU_CORES = float(CPU_QUOTA) / float(CPU_PERIOD) if CPU_PERIOD > 0 else 0

# --- Global State ---
stats_data = []
stop_monitoring = threading.Event()

# --- Helper Functions --- (Keep parse_mem_string, monitor_container, load_config as is)
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
             return 0.0 if mem_str == '0b' else float(mem_str) / (1024.0*1024.0)
    except ValueError:
        return 0.0

def monitor_container(container_name_or_id, local_stats_data):
    """Polls docker stats for a given container."""
    print(f"Starting monitoring for {container_name_or_id}...")
    while not stop_monitoring.is_set():
        try:
            result = subprocess.run(
                ['docker', 'stats', '--no-stream', '--format', '{{.CPUPerc}},{{.MemUsage}}', container_name_or_id],
                capture_output=True, text=True, check=True, encoding='utf-8'
            )
            stdout = result.stdout.strip()
            if stdout and '--' not in stdout:
                cpu_str, mem_str = stdout.split(',')
                timestamp = time.time()
                mem_usage_mib = parse_mem_string(mem_str.split('/')[0])
                local_stats_data.append({
                    'time': timestamp, 'cpu_perc_str': cpu_str, 'mem_usage_mib': mem_usage_mib
                })
            elif not stdout:
                pass # Container might just be starting
        except subprocess.CalledProcessError:
            break # Expected when container stops
        except FileNotFoundError:
            print("Monitor Error: 'docker' command not found.")
            stop_monitoring.set(); break
        except Exception as e:
            print(f"Monitor: Error getting stats: {e}")
            time.sleep(0.5)
        time.sleep(1)
    print(f"Stopping monitoring for {container_name_or_id}.")

def load_config(config_path):
    """Loads configuration from a JSON file."""
    if not config_path.is_file():
        return None, f"Configuration file not found at {config_path}"
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        required_keys = ["user_code_dir_relative", "code_entrypoint", "power_assumptions"]
        if not all(key in config for key in required_keys):
            return None, "Missing required top-level key(s) in config."
        required_power_keys = ["cpu_per_core_watt", "ram_per_gb_watt", "baseline_container_watt"]
        if not all(key in config["power_assumptions"] for key in required_power_keys):
             return None, "Missing required key(s) under 'power_assumptions'."
        return config, None
    except json.JSONDecodeError as e:
        return None, f"Error decoding JSON {config_path}: {e}"
    except Exception as e:
        return None, f"Error loading config {config_path}: {e}"

# --- Main Execution ---
def run():
    """Runs the containerized code execution."""
    results = {
        'success': False, 'error': None, 'exit_code': -1, 'logs': None,
        'runtime_seconds': 0.0, 'samples_collected': 0, 'avg_cpu_percent': 0.0,
        'avg_mem_mib': 0.0, 'peak_mem_mib': 0.0, 'avg_power_watt': 0.0,
        'energy_kwh': 0.0, 'power_assumptions': None, 'raw_stats': []
    }
    SCRIPT_DIR = Path(__file__).parent.resolve()
    CONFIG_FILE_PATH = SCRIPT_DIR / "config.json"

    print(f"Loading configuration from: {CONFIG_FILE_PATH}")
    config, error = load_config(CONFIG_FILE_PATH)
    if error:
        results['error'] = f"Configuration Error: {error}"; print(results['error']); return results
    results['power_assumptions'] = config['power_assumptions']

    try:
        USER_CODE_DIR_RELATIVE = config['user_code_dir_relative']
        CODE_ENTRYPOINT = config['code_entrypoint']
        POWER_CPU_PER_CORE_WATT = float(config['power_assumptions']['cpu_per_core_watt'])
        POWER_RAM_PER_GB_WATT = float(config['power_assumptions']['ram_per_gb_watt'])
        POWER_BASELINE_CONTAINER_WATT = float(config['power_assumptions']['baseline_container_watt'])
        # Optionally read wait timeout from config
        container_wait_timeout_config = config.get("container_wait_timeout", 60) # Default 300s (5 min)
    except (KeyError, ValueError) as e:
         results['error'] = f"Config Data Error: Invalid or missing key/value: {e}"; print(results['error']); return results

    USER_CODE_DIR = (SCRIPT_DIR / USER_CODE_DIR_RELATIVE).resolve()
    if not USER_CODE_DIR.is_dir():
        results['error'] = f"User code directory not found: {USER_CODE_DIR}"; print(results['error']); return results

    print(f"Using user code from: {USER_CODE_DIR}")
    print(f"Entrypoint script: {CODE_ENTRYPOINT}")
    # Use the configured timeout
    container_wait_timeout = int(container_wait_timeout_config)
    print(f"Container wait timeout set to: {container_wait_timeout} seconds")


    client = None
    container = None
    monitor_thread = None
    run_duration = 0.0
    start_run_time = 0.0
    container_logs = "[Logs not retrieved]"
    container_exit_code = -1
    # Use unique names to avoid conflicts if previous cleanup failed
    container_name = f"{CONTAINER_NAME_PREFIX}{int(time.time())}"
    local_stats_data = []
    stop_monitoring.clear()

    try:
        try:
            client = docker.from_env()
            client.ping()
            print("Docker daemon connected.")
        except Exception as e:
            results['error'] = f"Docker Connection Error: {e}. Is Docker running?"; print(results['error']); return results

        # --- Build image if needed --- (Keep existing build logic)
        try:
            client.images.get(IMAGE_NAME)
            print(f"Docker image '{IMAGE_NAME}' found.")
        except docker.errors.ImageNotFound:
            print(f"Docker image '{IMAGE_NAME}' not found. Building...")
            try:
                dockerfile_path = SCRIPT_DIR / "python-sandbox.Dockerfile"
                if not dockerfile_path.is_file(): raise FileNotFoundError(f"Dockerfile missing: {dockerfile_path}")
                print(f"Using build context: {SCRIPT_DIR}")
                # Use API client for better build log streaming
                build_stream = client.api.build(
                    path=str(SCRIPT_DIR), dockerfile=str(dockerfile_path.name),
                    tag=IMAGE_NAME, rm=True, decode=True
                )
                last_log = None
                for chunk in build_stream:
                    if 'stream' in chunk:
                        line = chunk['stream'].strip();
                        if line: print(f" Build> {line}"); last_log = line
                    elif 'errorDetail' in chunk:
                        raise docker.errors.BuildError(chunk['errorDetail']['message'], build_log=last_log or "")
                print(f"Successfully built image '{IMAGE_NAME}'")
            except (docker.errors.BuildError, FileNotFoundError) as e:
                results['error'] = f"Docker Build Error: {e}"; print(results['error']); return results
            except Exception as e: # Catch other potential API issues
                 results['error'] = f"Unexpected Docker Build Error: {e}"; print(results['error']); return results


        # --- Run the container --- (Keep existing run logic)
        print(f"\nStarting container '{container_name}'...")
        start_run_time = time.time()
        container = client.containers.run(
            image=IMAGE_NAME, command=["python", CODE_ENTRYPOINT], name=container_name,
            detach=True, user='appuser', network_disabled=True,
            mem_limit=MEM_LIMIT, memswap_limit=MEM_LIMIT,
            cpu_period=CPU_PERIOD, cpu_quota=CPU_QUOTA,
            volumes={str(USER_CODE_DIR): {'bind': '/app', 'mode': 'ro'}},
            working_dir='/app', stdout=True, stderr=True, remove=False
        )
        print(f"Container '{container.name}' ({container.short_id}) started.")

        monitor_thread = threading.Thread(target=monitor_container, args=(container.name, local_stats_data))
        monitor_thread.start()

        print(f"Waiting for container to finish (timeout: {container_wait_timeout}s)...")
        try:
            # *** Main change is here in the except block ***
            result = container.wait(timeout=container_wait_timeout)
            container_exit_code = result.get('StatusCode', -1)
        # Catch specific requests timeout/connection errors, plus Docker API errors and standard TimeoutError
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError, docker.errors.APIError, TimeoutError) as e:
             # Provide a more specific message based on exception type if possible
             error_type = type(e).__name__
             print(f"Error waiting for container ({error_type}): {e}")
             container_exit_code = -99 # Indicate timeout or wait error
             try:
                 # Check status before trying to stop
                 container.reload()
                 print(f"Container status after wait error: {container.status}")
                 if container.status == 'running':
                     print("Container timed out or wait failed, attempting to stop...")
                     container.stop(timeout=10) # Short timeout for stop
             except Exception as stop_err:
                 print(f"Error trying to stop/check container after wait failure: {stop_err}")

        end_run_time = time.time()
        run_duration = end_run_time - start_run_time
        results['runtime_seconds'] = round(run_duration, 2)
        results['exit_code'] = container_exit_code
        print(f"Container finished or wait ended. Exit code: {container_exit_code}. Duration: {run_duration:.2f} seconds.")

        # --- Stop monitoring ---
        print("Stopping monitor thread...")
        stop_monitoring.set()
        if monitor_thread and monitor_thread.is_alive(): # Check thread exists before join
            monitor_thread.join(timeout=5)
            if monitor_thread.is_alive(): print("Warning: Monitor thread did not stop gracefully.")

        # --- Get logs --- (Keep existing log retrieval)
        print("\n--- Container Logs ---")
        try:
            container.reload() # Reload state before getting logs
            container_logs = container.logs().decode('utf-8', errors='replace')
            results['logs'] = container_logs
            print(container_logs if container_logs else "[No output]")
        except docker.errors.NotFound:
             results['logs'] = "[Error: Container not found during log retrieval]"
             print(results['logs'])
        except Exception as e:
            results['logs'] = f"[Error retrieving logs: {e}]"; print(results['logs'])
        print("--- End Logs ---")

        # --- Process statistics --- (Keep existing stats processing)
        print("\n--- Processing Stats & Energy Estimation ---")
        results['samples_collected'] = len(local_stats_data)
        if 'raw_stats' in results: results['raw_stats'] = local_stats_data

        if local_stats_data and run_duration > 0:
            total_cpu_perc_val, total_mem_mib, peak_mem_mib_calc = 0.0, 0.0, 0.0
            count = 0
            for stat in local_stats_data:
                try:
                    cpu_val = float(stat['cpu_perc_str'].replace('%', ''))
                    mem_val_mib = stat['mem_usage_mib']
                    if not math.isnan(cpu_val): total_cpu_perc_val += cpu_val
                    if not math.isnan(mem_val_mib):
                        total_mem_mib += mem_val_mib
                        peak_mem_mib_calc = max(peak_mem_mib_calc, mem_val_mib)
                    count += 1
                except (ValueError, KeyError) as e:
                    print(f"Warning: Could not parse stats entry: {stat} - Error: {e}")
            if count > 0:
                avg_cpu_perc_calc = total_cpu_perc_val / count
                avg_mem_mib_calc = total_mem_mib / count
                avg_mem_gb = avg_mem_mib_calc / 1024.0
                avg_cpu_power_watt = (avg_cpu_perc_calc/100.0)*ALLOCATED_CPU_CORES*POWER_CPU_PER_CORE_WATT
                avg_ram_power_watt = avg_mem_gb * POWER_RAM_PER_GB_WATT
                total_avg_power_watt_calc = avg_cpu_power_watt + avg_ram_power_watt + POWER_BASELINE_CONTAINER_WATT
                energy_kwh_calc = (total_avg_power_watt_calc / 1000.0) * (run_duration / 3600.0)

                results.update({
                    'avg_cpu_percent': round(avg_cpu_perc_calc, 2), 'avg_mem_mib': round(avg_mem_mib_calc, 2),
                    'peak_mem_mib': round(peak_mem_mib_calc, 2), 'avg_power_watt': round(total_avg_power_watt_calc, 3),
                    'energy_kwh': energy_kwh_calc, 'success': True
                })
                print(f"Stats: Samples={results['samples_collected']}, AvgCPU={results['avg_cpu_percent']}%, AvgMem={results['avg_mem_mib']} MiB")
                print(f"Energy: AvgPower={results['avg_power_watt']} W, Total={results['energy_kwh']:.9f} kWh")
            else: results['error'] = "Stats collected but count is zero or parsing failed."
        elif not local_stats_data: results['error'] = "No statistics were collected."
        else: results['error'] = "Run duration was zero or negative."
        if results['error'] and not results['success']: print(f"Stats Error: {results['error']}")
        print("--- End Stats & Energy Estimation ---")

    except Exception as e:
        error_trace = traceback.format_exc()
        results['error'] = f"An unexpected error occurred during run: {e}\n{error_trace}"
        print(results['error'])
        if monitor_thread and monitor_thread.is_alive():
            print("Stopping monitor thread due to error...")
            stop_monitoring.set(); monitor_thread.join(timeout=2)

    finally:
        # --- Cleanup --- (Keep existing cleanup)
        if container:
            try:
                print(f"\nCleaning up container '{container.name}'...")
                container.reload()
                if container.status == 'running':
                     print("Stopping running container during cleanup..."); container.stop(timeout=10); container.reload()
                print("Removing container...")
                container.remove(v=True) # v=True if anonymous volumes might be created
                print("Cleanup complete.")
            except docker.errors.NotFound: print("Container already removed.")
            except Exception as e:
                cleanup_error = f"Error during container cleanup: {e}"; print(cleanup_error)
                if not results['error']: results['error'] = cleanup_error
        return results

# --- Example Usage ---
if __name__ == "__main__":
    print("Running backend_runner directly for testing...")
    # Ensure config.json points to valid code and adjust timeout if needed in config
    run_results = run()
    print("\n--- Run Results ---")
    print(json.dumps(run_results, indent=4))
    if not run_results['success']: print("\nRun failed or encountered errors.")
    else: print("\nRun completed.")