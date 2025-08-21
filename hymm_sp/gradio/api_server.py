# api_server.py
from flask import Flask, request, jsonify
import os
import json
import time
from loguru import logger
import signal
import sys
import traceback

app = Flask(__name__)

# ========== Configuration ==========
STATE_FILE = "./gradio_results/current_index.txt"
TRIGGER_FILE = "./gradio_results/trigger.txt"  # Signal file agreed with worker
RESULT_DIR = "./gradio_results"  # Result file directory agreed with worker
SHUTDOWN_FILE = "./gradio_results/shutdown_worker.signal"  # Signal file to notify worker to shut down
MAX_WAIT_TIME = 30000  # Maximum seconds to wait for results
CHECK_INTERVAL = 1   # Interval seconds to check result files
API_PORT = int(os.getenv("API_PORT"))

os.makedirs(RESULT_DIR, exist_ok=True)
os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)

# Ensure initial state file exists
if not os.path.exists(STATE_FILE):
    with open(STATE_FILE, 'w') as f:
        f.write("0")

def get_total_samples():
    """Get total number of samples, which needs to be dynamically calculated or hardcoded based on your CSV file"""
    # Example: Hardcode or read from CSV
    # import pandas as pd
    # df = pd.read_csv("/path/to/your/csv.csv")
    # return len(df)
    return 1000  # Please replace with actual value

@app.route('/health', methods=['GET'])
def health():
    # Simple health check: Check if result directory exists
    worker_ready = os.path.exists(RESULT_DIR) and os.access(RESULT_DIR, os.W_OK)
    return jsonify({"status": "healthy" if worker_ready else "unhealthy", "worker_ready": worker_ready})

def get_current_index():
    """Read current index from state file"""
    try:
        with open(STATE_FILE, 'r') as f:
            return int(f.read().strip())
    except (FileNotFoundError, ValueError):
        return 0

def update_current_index(new_index):
    """Update index in state file"""
    try:
        with open(STATE_FILE, 'w') as f:
            f.write(str(new_index))
    except Exception as e:
        logger.error(f"Failed to update state file: {e}")
        raise

@app.route('/generate_next', methods=['POST'])
def generate_next():
    # Get request data
    request_data = request.get_json() or {}
    
    # Check if there are custom parameters
    custom_params = request_data.get("custom_params")
    
    if custom_params:
        # Process custom parameter generation
        logger.info("Received custom parameter generation request")
        logger.info(f"Custom parameters: {json.dumps(custom_params, indent=2, ensure_ascii=False)}")
        
        # Validate required parameters
        required_fields = []
        missing_fields = [field for field in required_fields if field not in custom_params]
        if missing_fields:
            return jsonify({"error": f"Missing required fields: {missing_fields}"}), 400
            
        # Use special index to identify custom generation
        current_index = -1  # -1 indicates custom generation
        trigger_data = {
            "index": current_index,
            "custom_params": custom_params
        }
        
        logger.info("Custom generation parameters validated successfully")
        
    else:
        # Process automatic sequential generation
        current_index = get_current_index()
        total_samples = get_total_samples()  # Get actual total

        if current_index >= total_samples:
            return jsonify({"error": "All samples generated", "index": current_index}), 400

        logger.info(f"API received request. Triggering generation for index: {current_index}")
        trigger_data = {"index": current_index}
    
    # Write signal file (atomic operation)
    trigger_tmp = TRIGGER_FILE + ".tmp"
    try:
        with open(trigger_tmp, 'w') as f:
            json.dump(trigger_data, f, indent=2, ensure_ascii=False)
        os.replace(trigger_tmp, TRIGGER_FILE)  # Atomically replace/create
        time.sleep(100)
        logger.info(f"Trigger file {TRIGGER_FILE} written.")
        logger.info(f"Trigger data: {json.dumps(trigger_data, indent=2, ensure_ascii=False)}")
    except Exception as e:
        logger.error(f"Failed to write trigger file: {e}")
        logger.error(traceback.format_exc())
        return jsonify({"error": f"Failed to trigger generation: {e}"}), 500

    # Wait for result file
    if current_index == -1:
        # Custom generation uses special result file name
        result_file_path = os.path.join(RESULT_DIR, f"result_custom.json")
    else:
        result_file_path = os.path.join(RESULT_DIR, f"result_{current_index}.json")
        
    start_time = time.time()
    while time.time() - start_time < MAX_WAIT_TIME:
        if os.path.exists(result_file_path):
            try:
                with open(result_file_path, 'r') as f_res:
                    result = json.load(f_res)
                logger.info(f"Result file found and read: {result_file_path}")
                
                # Clean up result file
                os.remove(result_file_path)
                logger.info(f"Result file {result_file_path} removed.")
                
                # If automatic sequential generation, update index
                if current_index != -1:
                    update_current_index(current_index + 1)
                    logger.info(f"Index updated to {current_index + 1}")
                
                # Return result
                if "error" in result:
                     return jsonify(result), 500  # Return worker's error
                return jsonify(result)
                
            except Exception as e:
                logger.error(f"Error reading/parsing result file {result_file_path}: {e}")
                logger.error(traceback.format_exc())
                # Clean up possibly corrupted file
                if os.path.exists(result_file_path):
                    try:
                        os.remove(result_file_path)
                    except Exception as remove_e:
                        logger.warning(f"Failed to remove corrupted result file: {remove_e}")
                return jsonify({"error": f"Error processing result: {e}"}), 500
        
        time.sleep(CHECK_INTERVAL)

    # Timeout handling
    # logger.error(f"Timeout waiting for result file {result_file_path}")
    # Try to clean up signal file in case worker didn't process it
    if os.path.exists(TRIGGER_FILE):
        try:
            os.remove(TRIGGER_FILE)
            logger.info(f"Removed stale trigger file {TRIGGER_FILE} due to timeout.")
        except Exception as e:
            logger.warning(f"Could not remove stale trigger file {TRIGGER_FILE}: {e}")
            
    return jsonify({"error": "Timeout waiting for video generation"}), 500

# Notify worker when gracefully shutting down API server
def signal_handler(sig, frame):
    logger.info("Received shutdown signal for API server...")
    # Notify worker to shut down
    try:
        with open(SHUTDOWN_FILE, 'w') as f:
            f.write("shutdown")
        logger.info("Shutdown signal sent to worker.")
    except Exception as e:
        logger.error(f"Failed to send shutdown signal to worker: {e}")
    sys.exit(0)

if __name__ == "__main__":
    # Register signal handlers
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    try:
        logger.info("🔥 Starting Flask API server on port 8081...")
        # Note: No longer need threaded=False, use_reloader=False as there's no distributed interference
        app.run(host="0.0.0.0", port=API_PORT, debug=False)
    except Exception as e:
        logger.error(f"Fatal error in API server: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)
