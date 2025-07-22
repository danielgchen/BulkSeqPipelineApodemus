import time
import argparse
import random

STATUS_FILE = "history.log"
def write_status(message: str) -> None:
    # write a message to the status file
    with open(STATUS_FILE, "a") as f:
        f.write(f"{message}\n")

def simulate_pipeline(config_file):
    """
    Simulates a bioinformatics pipeline.
    It prints status messages that the web interface will parse.
    """
    # Define the modules in the pipeline
    modules = [
        "Quality_Control",
        "Adapter_Trimming",
        "Alignment",
        "Post-Alignment_Processing",
        "Variant_Calling",
        "Annotation"
    ]
    
    write_status(f"INFO: Reading configuration from {config_file}")

    for module in modules:
        # Simulate some modules being skipped
        if random.random() < 0.1: # 10% chance to skip
            write_status(f"STATUS: {module} skipped")
            continue

        # Mark the module as in progress
        write_status(f"STATUS: {module} in_progress")
        time.sleep(2)
        
        # Mark the module as finished
        write_status(f"STATUS: {module} finished")
        time.sleep(2)

    write_status("INFO: Pipeline finished.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulated Bioinformatics Pipeline")
    parser.add_argument('-c', '--configuration_file', required=True, help="Path to the configuration file.")
    args = parser.parse_args()
    
    simulate_pipeline(args.configuration_file)
