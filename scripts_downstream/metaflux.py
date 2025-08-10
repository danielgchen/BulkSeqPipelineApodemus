import argparse
import datetime
import logging
import os
import pandas as pd
import subprocess
from typing import Dict, Tuple
# private packages
from constants import *

# create a logger object writing to the given file
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# create constants for database locations
TIMESTAMP = round(datetime.datetime.now().timestamp())

def setup_logger(filename: str) -> None:
    # set up logger to write to a given file
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s: %(message)s",
        handlers=[logging.FileHandler(filename)],
    )
    logger.info("Initialized logging")
    
def construct_filenames(output_directory: str, timestamp: int) -> Tuple[str, str]:
    expr_mtx = os.path.join(output_directory, f"{timestamp}.expr.csv")
    out_fn = os.path.join(output_directory, f"{timestamp}.metaflux.csv")
    subsystem_mtx = os.path.join(output_directory, f"{timestamp}.metaflux.subsystem.csv")
    return expr_mtx, out_fn, subsystem_mtx

def validate_filenames(filenames: Tuple[str, str]) -> bool:
    any_exist = False
    for filename in filenames:
        if os.path.exists(filename):
            any_exist = True
            break
    return any_exist
    
def validate_timestamp(output_directory: str, timestamp: int) -> Tuple[str, int]:
    logger.info(f"Identifying a valid timestamp to utilize for file names starting from {timestamp}...")
    # continously find a timestamp that works best
    filenames = construct_filenames(output_directory=output_directory, timestamp=timestamp)
    # instantiate tracker t
    n_tries = 0
    while validate_filenames(filenames=filenames):
        # reconstruct the filenames based on the new stamp
        timestamp += 1
        filenames = construct_filenames(output_directory=output_directory, timestamp=timestamp)
        # upcount the number of tries, break after five tries
        n_tries += 1
        if n_tries >= 5:
            break
    if validate_filenames(filenames=filenames):
        raise ValueError("Temporary filename cannot be found")
    logger.info(f"Valid timestamp {timestamp} has been found.")
    return timestamp

def run(command: str) -> subprocess.Popen:
    # run a command in the shell and return the process
    logger.info(f"Running `{command}`...")
    process = subprocess.Popen(command, shell=True)
    return process

def cpm(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Transform data into counts-per-million (CPM)")
    df /= df.sum(0)
    df *= 1e6
    return df
    
def run_metafluxr(expr_mtx: str, medium: str, out_fn: str):
    logger.info(f"Running metaflux in R...")
    dirname = os.path.dirname(__file__)
    filename = os.path.join(dirname, "metaflux.R")
    process = run(f"Rscript {filename} {expr_mtx} {medium} {out_fn}")
    process.wait()
    # process.wait()
    logger.info("metaflux.R has finished running.")

def process_metafluxr(meta_mtx: str, subsystem_mtx: str):
    # read in the flux data
    df = pd.read_csv(meta_mtx, index_col=0)
    # read in human GEM annotation to add on equation annotation
    human_gem = pd.read_csv(GEM_FILE, index_col=0)
    id2eq = human_gem.set_index('ID')['EQUATION']
    # rename with the actual equation
    df.index = df.index.map(id2eq)
    # save the metabolic matrix
    df.to_csv(meta_mtx)
    # retrieve subsystem scores
    df['subsystem'] = df.index.map(human_gem.set_index('EQUATION')['SUBSYSTEM'])
    df_subsystem = df.groupby('subsystem').mean()
    # write the subsystem averaged matrix
    df_subsystem.to_csv(subsystem_mtx)        

def main():
    # read in command line arguments
    parser = argparse.ArgumentParser(description="CellPhoneDB Autocrine Signaling Quantification")
    parser.add_argument(
        "-i",
        "--expression_file",
        type=str,
        default="/home/dchen2/TMP/expr.csv",
        help="Path to the expression matrix (rows are genes, columns are samples), or use the transpose option",
    )
    parser.add_argument(
        "-m",
        "--medium",
        type=str,
        default="cell_medium",
        help="Name of the medium file to utilize for the metabolic GEM",
    )
    parser.add_argument(
        "-o",
        "--output_directory",
        type=str,
        default="/home/dchen2/TMP/",
        help="Path to the output directory to drop results in",
    )
    parser.add_argument(
        "-t",
        "--transpose",
        type=bool,
        default=False,
        help="Whether to transpose the expression matrix, i.e. if it is rows are samples and columns are genes"
    )
    parser.add_argument(
        "-c",
        "--cpmtransform",
        type=bool,
        default=False,
        help="Whether to transform the expression to counts-per-million (CPM)"
    )
    parser.add_argument(
        "-l",
        "--logging_file",
        type=str,
        default="BulkPipelineDownstream.metaflux.log",
        help="Path to log file for this downstream task"
    )
    args = parser.parse_args()
    # setup logger
    setup_logger(filename=args.logging_file)

    # create the output directory if it does not already exist and find an appropriate timestamp
    os.makedirs(args.output_directory, exist_ok=True)
    timestamp = validate_timestamp(output_directory=args.output_directory, timestamp=TIMESTAMP)
    expr_mtx, out_fn, subsystem_mtx = construct_filenames(output_directory=args.output_directory, timestamp=timestamp)
    
    # write down the expression matrix
    expr_mtx_data = pd.read_csv(args.expression_file, index_col=0)
    if args.transpose: expr_mtx_data = expr_mtx_data.T
    if args.cpmtransform: expr_mtx_data = cpm(expr_mtx_data)
    expr_mtx_data.to_csv(expr_mtx)

    # sprint through the pipeline
    run_metafluxr(expr_mtx=expr_mtx, medium=args.medium, out_fn=out_fn)
    process_metafluxr(meta_mtx=out_fn, subsystem_mtx=subsystem_mtx)
    
if __name__ == "__main__":
    main()