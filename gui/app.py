import os
import subprocess
from flask import Flask, render_template, request, jsonify
import yaml

# define the path for the status file that will be written to by the main script (this is the same as constants)
DIRECTORY = os.path.dirname(".")
STATUS_FILE = os.path.join(DIRECTORY, "status.log")
LOG_FILE = os.path.join(DIRECTORY, "BulkPipeline.log")
# clear the previous status log
for filename in [STATUS_FILE, LOG_FILE]:
    if os.path.exists(filename):
        os.remove(filename)
    open(filename, 'w').close()

# initialize the flask application
app = Flask(__name__, static_folder="static")

# main route for hosting the html
@app.route("/")
def index():
    """
    Renders the main HTML page.
    This function is called when a user navigates to the root URL.
    """
    # clear the log file on page load for a fresh start if it exists
    if os.path.exists(LOG_FILE):
        os.remove(LOG_FILE)
    # rend the main page of the GUI
    return render_template("index.html")

# handles file submission and runs the pipeline
@app.route("/run", methods=["POST"])
def run_script():
    try:
        # get the form data from the POST request
        data = request.json
        # locate the configuration file
        config_file = data["config_file"]
        # read in the YAML file to get the current parameters
        if not os.path.exists(config_file):
            return jsonify({"status": "error", "message": f"Configuration file {config_file} does not exist."}), 404
        with open(config_file, "r") as f:
            configs = yaml.safe_load(f)
        # update the configuration file with the new parameters
        for key, value in data.items():
            if key == "config_file":
                continue  # skip the config file key
            configs[key] = value
        # write the updated configuration back to the file
        config_file_updated = config_file.replace(".yaml", "_updated.yaml")
        with open(config_file_updated, "w") as f:
            yaml.safe_dump(configs, f, default_flow_style=False)
        # execute the main script with the updated configuration file
        subprocess.Popen(f"python ../main.py -c {config_file_updated} -l {LOG_FILE}", shell=True)
        # return a success message to the frontend if the process starts successfully
        return jsonify(
            {
                "status": "success",
                "message": f"Process started. Check {LOG_FILE} for logs.",
            }
        )
    except Exception as e:
        # return an error message if something goes wrong
        return jsonify({"status": "error", "message": str(e)}), 500


# route to retrieve the status of the pipeline
@app.route("/status")
def status():
    log_content = ""
    status_content = ""
    try:
        with open(LOG_FILE, "r") as f:
            log_content = f.read()
        with open(STATUS_FILE, "r") as f:
            status_content = f.read()
    except:
        pass
    return jsonify({"log_content": log_content, "status_content": status_content})


# main execution block to run the Flask app
if __name__ == "__main__":
    # retrieve host to run on remote server otherwise use local host
    try:
        host = subprocess.check_output("hostname -i", shell=True).decode().strip()
    except:
        # fall back to localhost
        host = "127.0.0.1"
    # run the Flask app in debug mode for development
    # unique digits of the sum of amodeus in binary = 7722526
    ports = [7256, 5000, 5001, 5002, 5003, 5004, 5005, 5006, 5007, 5008, 5009, 5010]
    for port in ports:
        try:
            app.run(host=host, port=port, debug=True)
            break  # exit the loop if the app runs successfully
        except Exception as e:
            print(f"Port {port} is in use, trying next port... Error: {e}")