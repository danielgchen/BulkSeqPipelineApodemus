import argparse
import datetime
import logging
import os
import pandas as pd
from typing import Dict, Tuple

# create a logger object writing to the given file
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# create constants for database locations
TF_DB = "/fh/fast/greenberg_p/user/dchen2/SAUCE/resources/pyscenic_data/allTFs_hg38.txt"
FEATHER_PT = "/fh/fast/greenberg_p/user/dchen2/SAUCE/resources/pyscenic_data/*.feather"
ANNO_FN = "/fh/fast/greenberg_p/user/dchen2/SAUCE/resources/pyscenic_data/motifs-v10nr_clust-nr.hgnc-m0.001-o0.0.tbl"
TIMESTAMP = round(datetime.datetime.now().timestamp())

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
    while os.path.exists(validate_filenames(filenames=filenames)):
        # reconstruct the filenames based on the new stamp
        timestamp += 1
        filenames = construct_filenames(output_directory=output_directory, timestamp=timestamp)
        # upcount the number of tries, break after five tries
        n_tries += 1
        if n_tries >= 5:
            break
    if os.path.exists(adj_fn):
        raise ValueError("Temporary filename cannot be found")
    logger.info(f"Valid timestamp {timestamp} has been found.")
    return timestamp

def run(command: str) -> subprocess.Popen:
    # run a command in the shell and return the process
    logger.info(f"Running `{command}`...")
    process = subprocess.Popen(command, shell=True)
    return process
    
def run_arboreto_mp(expr_mtx: str, out_fn: str, tf_db: str = TF_DB, n_cores: int = 1):
    logger.info(f"Running arboreto with multiprocessing enabled {n_cores} cores to find co-expression modules...")
    process = run(f"python arboreto_with_multiprocessing.py {expr_mtx} {tf_db} -o {out_fn} --num_workers {n_cores} --seed 0")
    process.wait()
    logger.info("arboreto has finished running.")

def run_tx_corr(expr_mtx: str, adj_fn: str, out_fn: str, feather_pat: str = FEATHER_PAT, anno_fn: str = ANNO_FN, n_cores: int = 1):
    logger.info(f"Infer motifs enriched in putative regulatory regions of GRNs with {n_cores} cores...")
    process = run(f"pyscenic ctx {adj_fn} {feather_pat} --annotations_fname {anno_fn} --expression_mtx_fname {expr_mtx} --output {out_fn} --mask_dropouts --num_workers {n_cores}")
    process.wait()
    logger.info("ctx has finished running.")

def run_aucell(expr_mtx: str, reg_fn: str, out_fn: str):
    logger.info(f"Compute TF activity via AUC of ranked gene expression...")
    process = run(f"pyscenic aucell {expr_mtx} {reg_fn} --output {out_fn} --num_workers {n_cores}")
    process.wait()
    logger.info("aucell has finished running.")

def main():
    # read in command line arguments
    parser = argparse.ArgumentParser(description="CellPhoneDB Autocrine Signaling Quantification")
    parser.add_argument(
        "-d",
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
    args = parser.parse_args()

    # create the output directory if it does not already exist and find an appropriate timestamp
    os.makedirs(args.output_directory, exist_ok=True)
    timestamp = validate_timestamp(output_directory=args.output_directory, timestamp=timestamp)
    expr_mtx, adj_fn, reg_fn, out_fn = construct_filenames(output_directory=args.output_directory, timestamp=timestamp)
    
    # write down the expression matrix
    expr_mtx_data = pd.read_csv(args.expression_file, index_col=0)
    if args.transpose: expr_mtx_data = expr_mtx_data.T
    expr_mtx_data.to_csv(expr_mtx)

    # sprint through the pipeline
    run_arboreto_mp(expr_mtx=expr_mtx, out_fn=adj_fn, n_cores=args.n_cores):
    run_tx_corr(expr_mtx=expr_mtx, adj_fn=adj_fn, out_fn=reg_fn, n_cores=args.n_cores)
    run_aucell(expr_mtx=expr_mtx, reg_fn=reg_fn, out_fn=out_fn)
    
    
if __name__ == "__main__":
    main()