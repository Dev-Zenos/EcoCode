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


IMAGE_NAME = "python-sandbox:latest"
CONTAINER_NAME_PREFIX = "test_run_container_" 

CPU_QUOTA = 50000   
CPU_PERIOD = 100000 
MEM_LIMIT = '256m'  


ALLOCATED_CPU_CORES = float(CPU_QUOTA) / float(CPU_PERIOD) if CPU_PERIOD > 0 else 0



stats_data = []
stop_monitoring = threading.Event()



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
    """
    Polls docker stats for a given container and appends to local_stats_data.
    Uses a local list to avoid global state issues if run concurrently (though not fully thread-safe without locks if run was called multiple times simultaneously).
    """
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
            if stdout and '--' not in stdout: 
                cpu_str, mem_str = stdout.split(',')
                timestamp = time.time()
                mem_usage_mib = parse_mem_string(mem_str.split('/')[0])
                local_stats_data.append({ 
                    'time': timestamp,
                    'cpu_perc_str': cpu_str,
                    'mem_usage_mib': mem_usage_mib
                 })
            elif not stdout:
                
                
                pass
        except subprocess.CalledProcessError:
            
            
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

def load_config(config_path):
    """Loads configuration from a JSON file."""
    if not config_path.is_file():
        
        return None, f"Configuration file not found at {config_path}"
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        required_keys = ["user_code_dir_relative", "code_entrypoint", "power_assumptions"]
        if not all(key in config for key in required_keys):
            return None, "Missing required top-level key(s) in configuration file."
        required_power_keys = ["cpu_per_core_watt", "ram_per_gb_watt", "baseline_container_watt"]
        if not all(key in config["power_assumptions"] for key in required_power_keys):
            return None, "Missing required key(s) under 'power_assumptions' in configuration file."
        return config, None 
    except json.JSONDecodeError as e:
        return None, f"Error decoding JSON from configuration file {config_path}: {e}"
    except Exception as e:
        return None, f"An unexpected error occurred while loading config {config_path}: {e}"



def run():
    """
    Runs the containerized code execution and returns performance/energy statistics.

    Returns:
        dict: A dictionary containing results:
              'success': bool, indicates overall success
              'error': str, error message if success is False
              'exit_code': int, container exit code (-1 if not run)
              'logs': str, container stdout/stderr logs
              'runtime_seconds': float
              'samples_collected': int
              'avg_cpu_percent': float
              'avg_mem_mib': float
              'peak_mem_mib': float
              'avg_power_watt': float
              'energy_kwh': float
              'power_assumptions': dict, the assumptions used for calculation
              'raw_stats': list, the list of collected stats samples (optional)
    """
    
    results = {
        'success': False,
        'error': None,
        'exit_code': -1,
        'logs': None,
        'runtime_seconds': 0.0,
        'samples_collected': 0,
        'avg_cpu_percent': 0.0,
        'avg_mem_mib': 0.0,
        'peak_mem_mib': 0.0,
        'avg_power_watt': 0.0,
        'energy_kwh': 0.0,
        'power_assumptions': None,
        'raw_stats': [] 
    }

    SCRIPT_DIR = Path(__file__).parent.resolve()
    CONFIG_FILE_PATH = SCRIPT_DIR / "config.json"

    
    print(f"Loading configuration from: {CONFIG_FILE_PATH}")
    config, error = load_config(CONFIG_FILE_PATH)
    if error:
        results['error'] = f"Configuration Error: {error}"
        print(results['error'])
        return results 

    results['power_assumptions'] = config['power_assumptions'] 

    
    try:
        USER_CODE_DIR_RELATIVE = config['user_code_dir_relative']
        CODE_ENTRYPOINT = config['code_entrypoint']
        POWER_CPU_PER_CORE_WATT = float(config['power_assumptions']['cpu_per_core_watt'])
        POWER_RAM_PER_GB_WATT = float(config['power_assumptions']['ram_per_gb_watt'])
        POWER_BASELINE_CONTAINER_WATT = float(config['power_assumptions']['baseline_container_watt'])
    except (KeyError, ValueError) as e:
         results['error'] = f"Config Data Error: Invalid or missing key/value: {e}"
         print(results['error'])
         return results

    
    USER_CODE_DIR = (SCRIPT_DIR / USER_CODE_DIR_RELATIVE).resolve()

    
    if not USER_CODE_DIR.is_dir():
        results['error'] = f"User code directory not found at resolved path: {USER_CODE_DIR} (from relative: '{USER_CODE_DIR_RELATIVE}')"
        print(results['error'])
        return results

    print(f"Using user code from: {USER_CODE_DIR}")
    print(f"Entrypoint script: {CODE_ENTRYPOINT}")
    print(f"Allocated CPU cores for container: {ALLOCATED_CPU_CORES}")

    client = None
    container = None
    monitor_thread = None
    run_duration = 0.0
    start_run_time = 0.0
    container_logs = "[Logs not retrieved]" 
    container_exit_code = -1

    
    container_name = "test"
    
    local_stats_data = [] 
    stop_monitoring.clear()

    try:
        
        try:
            client = docker.from_env()
            client.ping()
            print("Docker daemon connected.")
        except Exception as e:
            results['error'] = f"Docker Connection Error: {e}. Is Docker running?"
            print(results['error'])
            return results

        
        try:
            client.images.get(IMAGE_NAME)
            print(f"Docker image '{IMAGE_NAME}' found.")
        except docker.errors.ImageNotFound:
            print(f"Docker image '{IMAGE_NAME}' not found. Building...")
            try:
                dockerfile_path = SCRIPT_DIR / "python-sandbox.Dockerfile"
                if not dockerfile_path.is_file():
                    raise FileNotFoundError(f"Dockerfile not found at {dockerfile_path}")
                _, build_log = client.images.build(
                    path=str(SCRIPT_DIR),
                    dockerfile=str(dockerfile_path.name),
                    tag=IMAGE_NAME,
                    rm=True
                )
                print(f"Successfully built image '{IMAGE_NAME}'")
            except (docker.errors.BuildError, FileNotFoundError) as e:
                results['error'] = f"Docker Build Error: {e}"
                print(results['error'])
                
                return results

        
        print(f"\nStarting container '{container_name}'...")
        start_run_time = time.time()
        container = client.containers.run(
            image=IMAGE_NAME,
            command=["python", CODE_ENTRYPOINT],
            name=container_name, 
            detach=True,
            user='appuser',
            network_disabled=True,
            mem_limit=MEM_LIMIT,
            memswap_limit=MEM_LIMIT,
            cpu_period=CPU_PERIOD,
            cpu_quota=CPU_QUOTA,
            volumes={str(USER_CODE_DIR): {'bind': '/app', 'mode': 'ro'}},
            working_dir='/app',
            stdout=True,
            stderr=True,
            remove=False 
        )
        print(f"Container '{container.name}' ({container.short_id}) started.")

        
        monitor_thread = threading.Thread(target=monitor_container, args=(container.name, local_stats_data))
        monitor_thread.start()

        
        print("Waiting for container to finish...")
        
        container_wait_timeout = 60 
        try:
            result = container.wait(timeout=container_wait_timeout)
            container_exit_code = result.get('StatusCode', -1)
        except (docker.errors.APIError, TimeoutError) as e:
             print(f"Error waiting for container (maybe timed out or Docker error): {e}")
             container_exit_code = -99 
             
             try:
                 container.reload()
                 if container.status == 'running':
                     print("Container timed out or wait failed, attempting to stop...")
                     container.stop(timeout=10)
             except Exception as stop_err:
                 print(f"Error trying to stop container after wait failure: {stop_err}")

        end_run_time = time.time()
        run_duration = end_run_time - start_run_time
        results['runtime_seconds'] = round(run_duration, 2)
        results['exit_code'] = container_exit_code
        print(f"Container finished with exit code: {container_exit_code} in {run_duration:.2f} seconds.")

        
        print("Stopping monitor thread...")
        stop_monitoring.set()
        monitor_thread.join(timeout=5) 
        if monitor_thread.is_alive():
             print("Warning: Monitor thread did not stop gracefully.")

        
        print("\n--- Container Logs ---")
        try:
            
            container.reload()
            container_logs = container.logs().decode('utf-8', errors='replace')
            results['logs'] = container_logs
            print(container_logs if container_logs else "[No output]")
        except docker.errors.NotFound:
             print("Error retrieving logs: Container not found (may have been removed prematurely).")
             results['logs'] = "[Error: Container not found during log retrieval]"
        except Exception as e:
            print(f"Error retrieving logs: {e}")
            results['logs'] = f"[Error retrieving logs: {e}]"
        print("--- End Logs ---")


        
        print("\n--- Processing Stats & Energy Estimation ---")
        results['samples_collected'] = len(local_stats_data)
        if 'raw_stats' in results: results['raw_stats'] = local_stats_data 

        if local_stats_data and run_duration > 0:
            total_cpu_perc_val = 0.0
            total_mem_mib = 0.0
            peak_mem_mib_calc = 0.0
            count = 0

            for stat in local_stats_data:
                try:
                    cpu_val = float(stat['cpu_perc_str'].replace('%', ''))
                    if not math.isnan(cpu_val): total_cpu_perc_val += cpu_val
                    else: print(f"Warning: NaN value encountered for CPU percentage: {stat['cpu_perc_str']}")

                    mem_val_mib = stat['mem_usage_mib']
                    if not math.isnan(mem_val_mib):
                        total_mem_mib += mem_val_mib
                        if mem_val_mib > peak_mem_mib_calc: peak_mem_mib_calc = mem_val_mib
                    else: print(f"Warning: NaN value encountered for Memory usage: {stat['mem_usage_mib']}")
                    count += 1
                except (ValueError, KeyError) as e:
                    print(f"Warning: Could not parse stats entry: {stat} - Error: {e}")
                    continue

            if count > 0:
                avg_cpu_perc_calc = total_cpu_perc_val / count
                avg_mem_mib_calc = total_mem_mib / count
                avg_mem_gb = avg_mem_mib_calc / 1024.0

                
                avg_cpu_power_watt = (avg_cpu_perc_calc / 100.0) * ALLOCATED_CPU_CORES * POWER_CPU_PER_CORE_WATT
                avg_ram_power_watt = avg_mem_gb * POWER_RAM_PER_GB_WATT
                total_avg_power_watt_calc = avg_cpu_power_watt + avg_ram_power_watt + POWER_BASELINE_CONTAINER_WATT

                energy_kwh_calc = (total_avg_power_watt_calc / 1000.0) * (run_duration / 3600.0)

                
                results['avg_cpu_percent'] = round(avg_cpu_perc_calc, 2)
                results['avg_mem_mib'] = round(avg_mem_mib_calc, 2)
                results['peak_mem_mib'] = round(peak_mem_mib_calc, 2)
                results['avg_power_watt'] = round(total_avg_power_watt_calc, 3)
                results['energy_kwh'] = energy_kwh_calc

                print(f"Number of Stat Samples: {results['samples_collected']}")
                print(f"Average CPU Usage:      {results['avg_cpu_percent']}%")
                print(f"Average Memory Usage:   {results['avg_mem_mib']} MiB")
                print(f"Peak Memory Usage:      {results['peak_mem_mib']} MiB")
                print(f"Estimated Average Power:{results['avg_power_watt']} Watts")
                print(f"ESTIMATED ENERGY USAGE: {results['energy_kwh']:.9f} kWh")
                results['success'] = True 
            else:
                 results['error'] = "Stats collected but count is zero or parsing failed."
                 print(results['error'])

        elif not local_stats_data:
             results['error'] = "No statistics were collected."
             print(results['error'])
        else: 
             results['error'] = "Run duration was zero or negative, cannot calculate energy."
             print(results['error'])

        print("--- End Stats & Energy Estimation ---")

    
    except Exception as e:
        
        error_trace = traceback.format_exc()
        results['error'] = f"An unexpected error occurred during run: {e}\n{error_trace}"
        print(results['error'])
        
        if monitor_thread and monitor_thread.is_alive():
            print("Stopping monitor thread due to error...")
            stop_monitoring.set()
            monitor_thread.join(timeout=2)

    
    finally:
        
        if container:
            try:
                
                print(f"\nCleaning up container '{container.name}'...")
                container.reload()
                if container.status == 'running':
                     print("Stopping running container...")
                     container.stop(timeout=10)
                     container.reload() 
                print("Removing container...")
                container.remove(v=True) 
                print("Cleanup complete.")
            except docker.errors.NotFound:
                print("Container already removed or not found during cleanup.")
            except Exception as e:
                cleanup_error = f"Error during container cleanup: {e}"
                print(cleanup_error)
                
                if not results['error']:
                    results['error'] = cleanup_error
        
        return results


if __name__ == "__main__":
    print("Running backend_runner directly for testing...")
    run_results = run()
    print("\n--- Run Results ---")
    
    print(json.dumps(run_results, indent=4))

    if not run_results['success']:
        print("\nRun failed or encountered errors.")
    else:
        print("\nRun completed.")