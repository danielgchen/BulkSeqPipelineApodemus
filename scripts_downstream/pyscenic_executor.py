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


def run(command: str) -> subprocess.Popen:
    # run a command in the shell and return the process
    logger.info(f"Running `{command}`...")
    process = subprocess.Popen(command, shell=True)
    return process
    
def construct_filenames(output_directory: str, timestamp: int) -> Tuple[str, str, str, str]:
    expr_mtx = os.path.join(output_directory, f"{timestamp}.expr.csv")
    adj_fn = os.path.join(output_directory, f"{timestamp}.adj.csv")
    reg_fn = os.path.join(output_directory, f"{timestamp}.reg.csv")
    out_fn = os.path.join(output_directory, f"{timestamp}.pyscenic.csv")
    return expr_mtx, adj_fn, reg_fn, out_fn

def validate_filenames(filenames: Tuple[str, str, str, str]) -> bool:
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
    
def run_arboreto_mp(expr_mtx: str, out_fn: str, tf_db: str = TF_DB, n_cores: int = 1):
    logger.info(f"Running arboreto with multiprocessing enabled {n_cores} cores to find co-expression modules...")
    dirname = os.path.dirname(__file__)
    filename = os.path.join(dirname, "arboreto_with_multiprocessing.py")
    process = run(f"python {filename} {expr_mtx} {tf_db} -o {out_fn} --num_workers {n_cores} --seed 0")
    process.wait()
    logger.info("arboreto has finished running.")

def run_tx_corr(expr_mtx: str, adj_fn: str, out_fn: str, feather_pat: str = FEATHER_PAT, anno_fn: str = ANNO_FN, n_cores: int = 1):
    logger.info(f"Infer motifs enriched in putative regulatory regions of GRNs with {n_cores} cores...")
    process = run(f"pyscenic ctx {adj_fn} {feather_pat} --annotations_fname {anno_fn} --expression_mtx_fname {expr_mtx} --output {out_fn} --mask_dropouts --num_workers {n_cores}")
    process.wait()
    logger.info("ctx has finished running.")

def run_aucell(expr_mtx: str, reg_fn: str, out_fn: str, n_cores: int = 1):
    logger.info(f"Compute TF activity via AUC of ranked gene expression...")
    process = run(f"pyscenic aucell {expr_mtx} {reg_fn} --output {out_fn} --num_workers {n_cores}")
    process.wait()
    logger.info("aucell has finished running.")

def main():
    # read in command line arguments
    parser = argparse.ArgumentParser(description="CellPhoneDB Autocrine Signaling Quantification")
    parser.add_argument(
        "-i",
        "--expression_file",
        type=str,
        default="/home/dchen2/TMP/expr.csv",
        help="Path to the expression matrix (rows are samples, columns are genes), or use the transpose option",
    )
    parser.add_argument(
        "-o",
        "--output_directory",
        type=str,
        default="/home/dchen2/TMP/",
        help="Path to the output directory to drop results in",
    )
    parser.add_argument(
        "-n",
        "--n_cores",
        type=int,
        default=10,
        help="Number of cores to utilize",
    )
    parser.add_argument(
        "-t",
        "--transpose",
        type=bool,
        default=False,
        help="Whether to transpose the expression matrix, i.e. if it is rows are genes and columns are samples"
    )
    parser.add_argument(
        "-l",
        "--logging_file",
        type=str,
        default="BulkPipelineDownstream.pyscenic.log",
        help="Path to log file for this downstream task"
    )
    args = parser.parse_args()
    # setup logger
    setup_logger(filename=args.logging_file)

    # create the output directory if it does not already exist and find an appropriate timestamp
    os.makedirs(args.output_directory, exist_ok=True)
    timestamp = validate_timestamp(output_directory=args.output_directory, timestamp=TIMESTAMP)
    expr_mtx, adj_fn, reg_fn, out_fn = construct_filenames(output_directory=args.output_directory, timestamp=timestamp)
    
    # write down the expression matrix
    expr_mtx_data = pd.read_csv(args.expression_file, index_col=0)
    if args.transpose: expr_mtx_data = expr_mtx_data.T
    expr_mtx_data.to_csv(expr_mtx)

    # sprint through the pipeline
    run_arboreto_mp(expr_mtx=expr_mtx, out_fn=adj_fn, n_cores=args.n_cores)
    run_tx_corr(expr_mtx=expr_mtx, adj_fn=adj_fn, out_fn=reg_fn, n_cores=args.n_cores)
    run_aucell(expr_mtx=expr_mtx, reg_fn=reg_fn, out_fn=out_fn, n_cores=args.n_cores)
    # remove temporary files
    for fn in [adj_fn, reg_fn]:
        os.remove(fn)
    
if __name__ == "__main__":
    main()