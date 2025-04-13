import backend_runner as backend
from flask import Flask, jsonify, request
from flask_cors import CORS
import requests
import re
import os
import zipfile
import io
from urllib.parse import urlparse
from datetime import datetime
import json
from pathlib import Path
import shutil

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "http://localhost:3000"}})

CONFIG_FILE_PATH = os.path.join(os.path.dirname(__file__), 'config.json')

@app.route('/', methods=['GET'])
def test():
    return "Hello, World!"


@app.route('/upload_data', methods=['POST'])
def receive():
    processed_data = None
    if request.is_json:
            try:
                data = request.get_json()
                processed_data = {'received': data, 'status': 'success'}
                print(f"Received data name: {data}")
                return generate_sandbox_code(data)

            except Exception as e:
                print(f"Error processing data: {e}")
                return jsonify({'error': 'Invalid JSON'}), 400
    else:
         return jsonify({'error': 'Method Not Allowed'}), 405

def generate_sandbox_code(data):
    repo_name = clone_github_repo(data['data']['repo_url'])
    if not repo_name:
        return jsonify({'error': 'Failed to clone repository'}), 400
    data['data']['user_code_dir_relative'] += "/" + repo_name 
    config_updated = update_config(data['data'])
    if not config_updated:
        return jsonify({'error': 'Failed to update configuration'}), 400
    # Call the backend runner with the received data
    run_results = backend.run() # This now returns a dictionary
    print(f"Backend runner finished. Success status: {run_results.get('success')}")

        # --- 4. Process and Return Results ---
    if run_results and run_results.get('success'):
            # Backend execution was successful, return the stats
            # Select which stats to return in the API response
            response_data = {
                'status': 'success',
                'message': 'Code analysis completed.',
                'results': {
                    'exit_code': run_results.get('exit_code'),
                    'runtime_seconds': run_results.get('runtime_seconds'),
                    'samples_collected': run_results.get('samples_collected'),
                    'avg_cpu_percent': run_results.get('avg_cpu_percent'),
                    'avg_mem_mib': run_results.get('avg_mem_mib'),
                    'peak_mem_mib': run_results.get('peak_mem_mib'),
                    'avg_power_watt': run_results.get('avg_power_watt'),
                    'energy_kwh': run_results.get('energy_kwh'),
                    'power_assumptions_used': run_results.get('power_assumptions'),
                    'co2_rate': data['data']['co2_rate'],
                    # Optionally include logs: 'logs': run_results.get('logs', '[Logs not available]')
                }
            }
            delete_directory_force(data['data']['user_code_dir_relative']) # Clean up the cloned repo
            return jsonify(response_data), 200 # HTTP 200 OK

    else:
            # Backend execution failed or didn't return expected structure
            error_message = run_results.get('error', 'Unknown error during backend execution.') if run_results else 'Backend runner did not return results.'
            print(f"Backend execution failed: {error_message}") # Log the error server-side
            # Return the error reported by the backend runner
            return jsonify({'error': f'Backend execution failed: {error_message}'}), 500 # HTTP 500 Internal Server Error
    

def update_config(data):
    if not isinstance(data, dict):
        print("Error: Input data must be a dictionary.")
        return None

    # --- 1. Define Required Structure and Types ---
    # This defines the allowed keys and expected types at each level.
    # We'll validate the keys *provided* in the input data against this structure.
    valid_structure = {
        "user_code_dir_relative": str,
        "code_entrypoint": str,
        "power_assumptions": {
            "notes": str,
            "cpu_per_core_watt": (float, int), # Allow float or int
            "ram_per_gb_watt": (float, int),
            "baseline_container_watt": (float, int)
        },
        "repo_url": str,
        "co2_rate": (float, int), # Allow float or int
    }

    # --- 2. Validate Input Data Structure and Types ---
    for key, value in data.items():
        if key not in valid_structure:
            print(f"Error: Invalid top-level key provided in update data: '{key}'")
            return None

        expected_type = valid_structure[key]

        # Handle nested 'power_assumptions' validation
        if key == "power_assumptions":
            if not isinstance(value, dict):
                print(f"Error: Value for '{key}' must be a dictionary.")
                return None
            # Validate nested keys within power_assumptions
            nested_valid_structure = expected_type # expected_type is the nested dict structure
            for nested_key, nested_value in value.items():
                if nested_key not in nested_valid_structure:
                    print(f"Error: Invalid key provided under 'power_assumptions': '{nested_key}'")
                    return None
                expected_nested_type = nested_valid_structure[nested_key]
                if not isinstance(nested_value, expected_nested_type):
                    print(f"Error: Invalid type for 'power_assumptions.{nested_key}'. Expected {expected_nested_type}, got {type(nested_value)}.")
                    return None
        # Handle top-level keys validation
        elif not isinstance(value, expected_type):
            print(f"Error: Invalid type for '{key}'. Expected {expected_type}, got {type(value)}.")
    # --- 3. Load Existing Config ---
    if not os.path.isfile(CONFIG_FILE_PATH):
        print(f"Error: Configuration file not found at {CONFIG_FILE_PATH}")
        return None

    try:
        with open(CONFIG_FILE_PATH, 'r', encoding='utf-8') as f:
            current_config = json.load(f)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from configuration file {CONFIG_FILE_PATH}: {e}")
        return None
    except IOError as e:
        print(f"Error reading configuration file {CONFIG_FILE_PATH}: {e}")
        return None

    # --- 4. Merge Validated Data into Existing Config ---
    # We need to merge carefully, especially for nested dicts
    for key, value in data.items():
        if key == "power_assumptions" and isinstance(value, dict):
            # Ensure the nested dict exists before trying to update it
            if "power_assumptions" not in current_config or not isinstance(current_config.get("power_assumptions"), dict):
                 current_config["power_assumptions"] = {} # Create if missing or wrong type
            current_config["power_assumptions"].update(value) # Update nested dict
        else:
            current_config[key] = value # Update top-level value

    # --- 5. Write Updated Config Back to File ---
    try:
        with open(CONFIG_FILE_PATH, 'w', encoding='utf-8') as f:
            json.dump(current_config, f, indent=4) # Use indent for readability
        print(f"Configuration file '{CONFIG_FILE_PATH}' updated successfully.")
        return True
    except IOError as e:
        print(f"Error writing updated configuration to file {CONFIG_FILE_PATH}: {e}")
        return None
    except Exception as e:
         print(f"An unexpected error occurred while writing config: {e}")
         return None
    

def clone_github_repo(repo_url, target_base_dir="../test/code"):
    """
    Validates a GitHub repository URL and downloads it into a subdirectory of the specified target directory,
    with the subdirectory named after the repository. No Git installation required.
    
    Parameters:
    -----------
    repo_url : str
        URL of the GitHub repository to clone
    target_base_dir : str
        Base target directory where a subdirectory will be created for the repository (default: '../test/code')
    
    Returns:
    --------
    str:
        Name of the created subdirectory if successful, None otherwise
    
    Raises:
    -------
    ValueError:
        If the provided URL is not a valid GitHub repository URL
    """
    print(f"[2025-04-13 07:26:19] Starting download process for: {repo_url}")
    
    # Validate GitHub repository URL format
    github_url_pattern = r'^https?://(?:www\.)?github\.com/[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?/[a-zA-Z0-9_.-]+/?$'
    
    if not re.match(github_url_pattern, repo_url):
        raise ValueError("Invalid GitHub repository URL format")
    
    # Extract owner and repository name from URL
    parsed_url = urlparse(repo_url)
    path_parts = parsed_url.path.strip('/').split('/')
    
    if len(path_parts) < 2:
        raise ValueError("URL does not contain owner and repository name")
        
    owner, repo = path_parts[0], path_parts[1]
    
    # Check if the repository exists
    api_url = f"https://api.github.com/repos/{owner}/{repo}"
    print(f"[2025-04-13 07:26:19] Validating repository: {api_url}")
    
    response = requests.get(api_url)
    
    if response.status_code != 200:
        raise ValueError(f"Repository {owner}/{repo} not found or not accessible")
    
    # Create base target directory if it doesn't exist
    os.makedirs(target_base_dir, exist_ok=True)
    
    # Create repository-specific subdirectory
    repo_directory = os.path.join(target_base_dir, repo)
    
    # If directory already exists, add timestamp to make it unique
    if os.path.exists(repo_directory):
        timestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S')
        repo_directory = f"{repo_directory}_{timestamp}"
        
    os.makedirs(repo_directory, exist_ok=True)
    print(f"[2025-04-13 07:26:19] Created repository directory: {repo_directory}")
    
    # Download the repository as a ZIP file
    zip_url = f"https://api.github.com/repos/{owner}/{repo}/zipball/main"
    print(f"[2025-04-13 07:26:19] Downloading repository from: {zip_url}")
    
    try:
        response = requests.get(zip_url, stream=True)
        
        # If main branch doesn't exist, try master branch
        if response.status_code != 200:
            zip_url = f"https://api.github.com/repos/{owner}/{repo}/zipball/master"
            print(f"[2025-04-13 07:26:19] Trying master branch instead: {zip_url}")
            response = requests.get(zip_url, stream=True)
            
        if response.status_code != 200:
            raise ValueError(f"Failed to download repository: HTTP {response.status_code}")
        
        # Extract ZIP file to target directory
        print(f"[2025-04-13 07:26:19] Extracting ZIP file to repository directory")
        z = zipfile.ZipFile(io.BytesIO(response.content))
        
        # Get the name of the top directory in the ZIP (GitHub adds a generated directory name)
        root_dir = z.namelist()[0]
        
        # Extract all files
        for file_info in z.infolist():
            # Skip the root directory itself
            if file_info.filename == root_dir:
                continue
                
            # Remove the root directory from the path
            if file_info.filename.startswith(root_dir):
                new_filename = file_info.filename[len(root_dir):]
                if new_filename:  # Skip if the new filename is empty
                    # Extract to repository directory
                    if file_info.is_dir():
                        os.makedirs(os.path.join(repo_directory, new_filename), exist_ok=True)
                    else:
                        extracted_path = z.extract(file_info, repo_directory)
                        # Rename the file to remove the root directory prefix
                        final_path = os.path.join(repo_directory, new_filename)
                        os.rename(
                            os.path.join(repo_directory, file_info.filename),
                            final_path
                        )
        
        print(f"[2025-04-13 07:26:19] Repository {repo_url} successfully downloaded to {repo_directory}")
        
        # Return just the name of the created directory, not the full path
        return os.path.basename(repo_directory)
        
    except requests.RequestException as e:
        print(f"[2025-04-13 07:26:19] Network error: {str(e)}")
        return None
    except zipfile.BadZipFile:
        print(f"[2025-04-13 07:26:19] Invalid ZIP file received")
        return None
    except Exception as e:
        print(f"[2025-04-13 07:26:19] Error extracting repository: {str(e)}")
        return None



def delete_directory_force(dir_path):
    """
    Deletes a directory and all its contents recursively.

    Args:
        dir_path (str or Path): The path to the directory to delete.

    Returns:
        bool: True if the directory was successfully deleted or didn't exist initially.
        str: An error message string if deletion failed, otherwise None.
    """
    # Convert to Path object for easier handling
    path_obj = Path(dir_path)

    # Check if the path exists first
    if not path_obj.exists():
        print(f"Directory '{dir_path}' does not exist. Nothing to delete.")
        return True, None # Considered successful as the desired state is achieved

    # Check if it's actually a directory (and not a file)
    if not path_obj.is_dir():
        error_msg = f"Error: Provided path '{dir_path}' is a file, not a directory."
        print(error_msg)
        return False, error_msg

    # --- Proceed with deletion ---
    try:
        # shutil.rmtree() deletes the directory and all its contents.
        # Be VERY CAREFUL with this command!
        shutil.rmtree(path_obj)
        print(f"Successfully deleted directory and all its contents: '{dir_path}'")
        return True, None
    except OSError as e:
        # Catch potential errors during deletion (e.g., permission errors)
        error_msg = f"Error deleting directory '{dir_path}': {e}"
        print(error_msg)
        return False, error_msg
    except Exception as e:
        # Catch any other unexpected errors
        error_msg = f"An unexpected error occurred while deleting '{dir_path}': {e}"
        print(error_msg)
        return False, error_msg
     

    
if __name__ == '__main__':
    app.run(port=1234, debug=True)