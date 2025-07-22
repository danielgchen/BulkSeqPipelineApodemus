import os
import json
import queue
import threading
import yaml
from flask import Flask, jsonify, render_template, request, Response

# Import your refactored pipeline logic
from pipeline import run_pipeline_threaded, PIPELINE_STEPS

app = Flask(__name__)
# A thread-safe queue to hold log messages from the pipeline
log_queue = queue.Queue()
HISTORY_FILE = "run_history.json"
CONFIG_FILE = "configs.yaml"

def get_history():
    """Reads the history of completed steps from a JSON file."""
    if not os.path.exists(HISTORY_FILE):
        return []
    with open(HISTORY_FILE, 'r') as f:
        return json.load(f)

def save_history(history_data):
    """Saves the history of completed steps."""
    with open(HISTORY_FILE, 'w') as f:
        json.dump(history_data, f)

@app.route('/')
def index():
    """Serves the main HTML page."""
    return render_template('index.html', pipeline_steps=PIPELINE_STEPS)

@app.route('/config', methods=['GET', 'POST'])
def handle_config():
    """Handles reading and writing the configs.yaml file."""
    if request.method == 'GET':
        with open(CONFIG_FILE, 'r') as f:
            return jsonify(yaml.safe_load(f))
    elif request.method == 'POST':
        data = request.json
        with open(CONFIG_FILE, 'w') as f:
            yaml.dump(data, f, sort_keys=False)
        return jsonify({"message": "Configuration saved successfully."})

@app.route('/history', methods=['GET', 'POST'])
def handle_history():
    """Handles reading and clearing the run history."""
    if request.method == 'GET':
        return jsonify(get_history())
    elif request.method == 'POST': # Used to clear history
        save_history([])
        return jsonify({"message": "History cleared."})

@app.route('/run', methods=['POST'])
def run_pipeline_endpoint():
    """Starts the pipeline execution in a background thread."""
    start_step = request.json.get('start_step')
    if not start_step:
        return jsonify({"error": "start_step is required"}), 400

    # Run the pipeline in a separate thread to avoid blocking the server
    thread = threading.Thread(target=run_pipeline_threaded, args=(start_step, log_queue, CONFIG_FILE, HISTORY_FILE))
    thread.start()
    return jsonify({"message": f"Pipeline started from step: {start_step}"})

@app.route('/stream')
def stream():
    """Streams log messages to the client using Server-Sent Events."""
    def event_stream():
        while True:
            # Wait for a message in the queue and send it to the client
            message = log_queue.get()
            yield f"data: {json.dumps(message)}\n\n"
    return Response(event_stream(), mimetype='text/event-stream')

if __name__ == '__main__':
    app.run(debug=True, threaded=True)