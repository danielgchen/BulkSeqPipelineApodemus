import os
import subprocess
from flask import Flask, render_template, request, jsonify, send_from_directory

# --- Basic Setup ---
# Initialize the Flask application
# We specify the static_folder to be 'static'
app = Flask(__name__, static_folder="static")
# Define the path for the log file
STATUS_FILE = "history.log"
LOG_FILE = "history.log"
# Define the path for the configuration file
CONFIG_FILE = "config.ini"


# --- Main Route ---
@app.route("/")
def index():
    """
    Renders the main HTML page.
    This function is called when a user navigates to the root URL.
    """
    # Clear the log file on page load for a fresh start
    if os.path.exists(LOG_FILE):
        os.remove(LOG_FILE)
    return render_template("index.html")


# --- Route to Run the Script ---
@app.route("/run", methods=["POST"])
def run_script():
    try:
        # Get the form data from the POST request
        data = request.json
        # TODO update this to read from the YAML file and then update with the new parameter values
        with open(CONFIG_FILE, "w") as f:
            for key, value in data.items():
                f.write(f"{key} = {value}\n")

        # --- Execute the Main Script ---
        subprocess.Popen(f"python main.py -c {CONFIG_FILE}", shell=True)

        # Return a success message to the frontend
        return jsonify(
            {
                "status": "success",
                "message": f"Process started. Check {LOG_FILE} for logs.",
            }
        )
    except Exception as e:
        # Return an error message if something goes wrong
        return jsonify({"status": "error", "message": str(e)}), 500


# --- Route to Get Status ---
@app.route("/status")
def status():
    if not os.path.exists(LOG_FILE):
        return jsonify({"log_content": ""})

    with open(LOG_FILE, "r") as f:
        content = f.read()
    return jsonify({"log_content": content})


# --- Main Execution ---
if __name__ == "__main__":
    # Run the Flask app in debug mode for development
    app.run(host="0.0.0.0", port=5001, debug=True)
