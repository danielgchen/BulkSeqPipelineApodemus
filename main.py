import argparse
import os
import subprocess
from glob import glob
import logging
import yaml
import pandas as pd
from typing import Dict, List, Tuple
from constants import *

# create a logger object writing to the given file
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def setup_logger(filename: str) -> None:
    # set up logger to write to a given file
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s: %(message)s",
        handlers=[logging.FileHandler(filename)],
    )
    logger.info("Initialized logging for BulkPipeline")


def load_configs(filename: str) -> Dict:
    # read in the configuration file
    logger.info(f"Loading configuration from {filename}")
    with open(filename, "r") as f:
        return yaml.safe_load(f)
    logger.info("Configuration loaded successfully")


def configure_config(configs: Dict) -> Dict:
    # build fastq suffixes for read1 and read2
    configs["r1_fastq_suffix"] = configs["r1"] + configs["fastq_suffix"]
    configs["r2_fastq_suffix"] = configs["r2"] + configs["fastq_suffix"]
    return configs


def identify_start_step(configs: Dict, pipeline_steps: List[str]) -> List[str]:
    # retrieve the requested step to start the pipeline from
    logger.info("Identifying starting step for the pipeline")
    pipeline_start_step = configs["pipeline_start_step"]
    # identify the index of the start step in the pipeline steps
    start_step_index = pipeline_steps.index(pipeline_start_step)
    # subset the pipeline steps based on the requested starting step
    relevant_steps = pipeline_steps[start_step_index:]
    return relevant_steps


def run(command: str) -> subprocess.Popen:
    # run a command in the shell and return the process
    logger.info(f"Running `{command}`...")
    process = subprocess.Popen(command, shell=True)
    return process


def run_fastqc(fastq_suffix: str, fastq_directory: str, output_directory: str) -> None:
    # identify all fastq files in the input directory
    os.makedirs(output_directory, exist_ok=True)
    filenames = glob(os.path.join(fastq_directory, f"*{fastq_suffix}"))
    # run FastQC on each fastq file
    logger.info(f"Running FastQC on {fastq_directory} N={len(filenames)} files")
    processes = [
        run(f"fastqc {filename} -o {output_directory}") for filename in filenames
    ]
    # wait for all processes to finish
    logger.info("Waiting for FastQC processes to finish...")
    for process in processes:
        process.wait()
    logger.info(
        f"FastQC completed successfully with outputs written to {output_directory}"
    )


def intake_adapter_freqs(filename: str) -> pd.Series:
    # read in the adapter frequencies from the file
    logger.info(f"Identifying dominant adapter from {filename}")
    adapter_freqs_raw = pd.read_table(filename, comment="#", header=None, index_col=0)
    adapter_freqs = adapter_freqs_raw[2].str.replace("%", "").astype(float)
    return adapter_freqs


def detect_adapters(
    r1_fastq_suffix: str,
    r2_fastq_suffix: str,
    fastq_suffix: str,
    fastq_directory: str,
    adapter_suffix: str,
    known_adapter_filename: str,
    known_adapter_suffix: str,
    output_directory: str,
) -> Tuple[str, str]:
    # identify all read1 fastq files in the input directory
    os.makedirs(output_directory, exist_ok=True)
    r1_filenames = glob(os.path.join(fastq_directory, f"*{r1_fastq_suffix}"))
    logger.info(f"Detecting adapters for {fastq_directory} N={len(r1_filenames)} files")
    processes = []
    for r1_filename in r1_filenames:
        # identify the corresponding read2 filename
        r2_filename = r1_filename.replace(r1_fastq_suffix, r2_fastq_suffix)
        # create the output filename for adapters
        adapter_filename = os.path.join(
            output_directory,
            os.path.basename(r1_filename).replace(r1_fastq_suffix, adapter_suffix),
        )
        # auto-detect adapters using bbmerge.sh
        process = run(
            f"bbmerge.sh -Xmx4g in1={r1_filename} in2={r2_filename} outa={adapter_filename}"
        )
        processes.append(process)
        # create the output filename for stats
        r1_stats = os.path.join(
            output_directory,
            os.path.basename(r1_filename).replace(fastq_suffix, known_adapter_suffix),
        )
        r2_stats = os.path.join(
            output_directory,
            os.path.basename(r2_filename).replace(fastq_suffix, known_adapter_suffix),
        )
        # run bbduk.sh to identify adapters guided by https://www.seqanswers.com/forum/bioinformatics/bioinformatics-aa/37399-introducing-bbduk-adapter-quality-trimming-and-filtering?q=ktrim
        process = run(
            f"bbduk.sh -Xmx4g in={r1_filename} stats={r1_stats} ref={known_adapter_filename}"
        )
        process = run(
            f"bbduk.sh -Xmx4g in={r2_filename} stats={r2_stats} ref={known_adapter_filename}"
        )
        processes.append(process)
    # wait for all processes to finish
    logger.info("Waiting for bbmerge.sh and bbduk.sh processes to finish...")
    for process in processes:
        process.wait()
    logger.info(
        f"Adapter detection completed successfully with outputs written to {output_directory}"
    )
    # identify the most common adapter sequence for read1 and read2
    r1_adapter_filenames = os.path.join(
        output_directory,
        "*" + r1_fastq_suffix.replace(fastq_suffix, known_adapter_suffix),
    )
    r2_adapter_filenames = os.path.join(
        output_directory,
        "*" + r1_fastq_suffix.replace(fastq_suffix, known_adapter_suffix),
    )
    # read in the adapter frequencies for read1 and read2
    r1_adapters, r2_adapters = [], []
    for r1_adapter_filename, r2_adapter_filename in zip(
        r1_adapter_filenames, r2_adapter_filenames
    ):
        r1_adapters.append(intake_adapter_freqs(filename=r1_adapter_filename))
        r2_adapters.append(intake_adapter_freqs(filename=r2_adapter_filename))
    # compute the most common adapter sequences
    r1_adapter = pd.concat(r1_adapters, axis=1).sum(axis=1).idxmax()
    r2_adapter = pd.concat(r2_adapters, axis=1).sum(axis=1).idxmax()
    logger.info(f"Most common adapter for read1: {r1_adapter}")
    logger.info(f"Most common adapter for read2: {r2_adapter}")
    return r1_adapter, r2_adapter


def trim_fastqs(
    r1_fastq_suffix: str,
    r2_fastq_suffix: str,
    fastq_suffix: str,
    fastq_directory: str,
    r1_adapter: str,
    r2_adapter: str,
    trimmed_suffix: str,
    trimmed_output_directory: str,
    qc_report_suffix: str,
    qc_reports_directory: str,
):
    # identify all read1 fastq files in the input directory
    os.makedirs(trimmed_output_directory, exist_ok=True)
    r1_filenames = glob(os.path.join(fastq_directory, f"*{r1_fastq_suffix}"))
    logger.info(
        f"Trimming adapters for {fastq_directory} N={len(r1_filenames)} files with {r1_adapter} and {r2_adapter}"
    )
    processes = []
    for r1_filename in r1_filenames:
        # identify the corresponding read2 filename
        r2_filename = r1_filename.replace(r1_fastq_suffix, r2_fastq_suffix)
        # create the output filename for read1 and read2 trimmed fastq files
        r1_trimmed = os.path.join(
            trimmed_output_directory,
            os.path.basename(r1_filename).replace(fastq_suffix, trimmed_suffix),
        )
        r2_trimmed = os.path.join(
            trimmed_output_directory,
            os.path.basename(r2_filename).replace(fastq_suffix, trimmed_suffix),
        )
        # create the output filename for cutadapt
        cutadapt_output = os.path.join(
            qc_reports_directory,
            os.path.basename(r1_filename).replace(fastq_suffix, qc_report_suffix),
        )
        # run cutadapt to trim adapters from read1 and read2
        run(
            f"cutadapt -a {r1_adapter} -A {r2_adapter} -m 20 -q 20 -o {r1_trimmed} -p {r2_trimmed} {r1_filename} {r2_filename} > {cutadapt_output}"
        )
    # wait for all processes to finish
    logger.info("Waiting for cutadapt trimming processes to finish...")
    for process in processes:
        process.wait()
    logger.info(
        f"Adapter trimming completed successfully with outputs written to {trimmed_output_directory} and {qc_reports_directory}"
    )


def map_fastqs(
    r1_fastq_suffix: str,
    r2_fastq_suffix: str,
    fastq_directory: str,
    mapped_output_directory: str,
    reference_genome: str,
    n_cores: int,
):
    # identify all read1 fastq files in the input directory
    os.makedirs(mapped_output_directory, exist_ok=True)
    r1_filenames = glob(os.path.join(fastq_directory, f"*{r1_fastq_suffix}"))
    logger.info(f"Mapping FASTQs in {fastq_directory} N={len(r1_filenames)} files")
    processes = []
    for r1_filename in r1_filenames:
        # identify the corresponding read2 filename
        r2_filename = r1_filename.replace(r1_fastq_suffix, r2_fastq_suffix)
        prefix = os.path.basename(r1_filename).split(r1_fastq_suffix)[0]
        process = run(
            f"STAR --runThreadN {int(n_cores)} --genomeDir {reference_genome} --readFilesIn {r1_filename} {r2_filename} --outSAMtype BAM SortedByCoordinate --outBAMsortingThreadN {n_cores} --outFileNamePrefix {prefix} --readFilesCommand gunzip -c"
        )
        processes.append(process)
    # wait for all processes to finish
    logger.info("Waiting for STAR mapping processes to finish...")
    for process in processes:
        process.wait()
    logger.info(
        f"Mapping completed successfully with outputs written to {mapped_output_directory}"
    )


def index_bams(bam_directory: str, bam_suffix: str):
    # identify all BAM files in the input directory
    bam_filenames = glob(f"{bam_directory}/*{bam_suffix}")
    logger.info(f"Indexing BAM files in {bam_directory} N={len(bam_filenames)} files")
    processes = []
    for bam_filename in bam_filenames:
        process = run(f"samtools index {bam_filename}")
        processes.append(process)
    # wait for all processes to finish
    logger.info("Waiting for SAMtools indexing processes to finish...")
    for process in processes:
        process.wait()
    logger.info(
        f"BAM indexing completed successfully with outputs written to {bam_directory}"
    )


def dedup_bams(
    bam_directory: str,
    bam_suffix: str,
    deduped_suffix: str,
    stats_suffix: str,
    stats_directory: str,
):
    # identify all BAM files in the input directory
    os.makedirs(stats_directory, exist_ok=True)
    bam_filenames = glob(f"{bam_directory}/*{bam_suffix}")
    logger.info(
        "Deduplicating BAM files in %s N=%d files", bam_directory, len(bam_filenames)
    )
    processes = []
    for bam_filename in bam_filenames:
        deduped_bam = bam_filename.replace(bam_suffix, deduped_suffix)
        deduped_stats = os.path.join(
            stats_directory,
            os.path.basename(bam_filename).replace(bam_suffix, stats_suffix),
        )
        process = run(
            f"java -Xmx16g -jar $PICARD MarkDuplicates I={bam_filename} O={deduped_bam} M={deduped_stats} REMOVE_DUPLICATES=true VALIDATION_STRINGENCY=LENIENT"
        )
        processes.append(process)
    # wait for all processes to finish
    logger.info("Waiting for PICARD deduplicating processes to finish...")
    for process in processes:
        process.wait()
    logger.info(
        "Deduplication completed successfully with outputs written to %s", bam_directory
    )


def qc_mapped_data(
    bam_suffix: str,
    bam_directory: str,
    qc_reports_directory: str,
    chr_stats_suffix: str,
    strand_inference_suffix: str,
    read_distribution_suffix: str,
    reference: str,
    reference_downsampled: str,
):
    # identify all BAM files in the input directory
    os.makedirs(qc_reports_directory, exist_ok=True)
    bam_filenames = glob(f"{bam_directory}/*{bam_suffix}")
    logger.info(
        "Performing quality control on BAM files in %s N=%d files",
        bam_directory,
        len(bam_filenames),
    )
    processes = []
    for bam_filename in bam_filenames:
        # samtools per chromsome stats
        stats_filename = os.path.join(
            qc_reports_directory,
            os.path.basename(bam_filename).replace(bam_suffix, chr_stats_suffix),
        )
        process = run(f"samtools idxstats {bam_filename} > {stats_filename}")
        processes.append(process)
        # rseqc strand inference
        strand_inference_filename = os.path.join(
            qc_reports_directory,
            os.path.basename(bam_filename).replace(bam_suffix, strand_inference_suffix),
        )
        process = run(
            f"infer_experiment.py -r {reference} -i {bam_filename} > {strand_inference_filename}"
        )
        processes.append(process)
        # rseqc read distribution
        read_distribution_filename = os.path.join(
            qc_reports_directory,
            os.path.basename(bam_filename).replace(
                bam_suffix, read_distribution_suffix
            ),
        )
        process = run(
            f"read_distribution.py -r {reference} -i {bam_filename} > {read_distribution_filename}"
        )
        processes.append(process)
    # wait for all processes to finish
    logger.info("Waiting for parallelized BAM quality control to finish...")
    for process in processes:
        process.wait()
    logger.info(
        "Parallelized BAM quality control completed successfully with outputs written to %s",
        qc_reports_directory,
    )
    # perform gene body coverage analysis
    logger.info("Running gene body coverage analysis...")
    bam_filenames_str = ",".join(bam_filenames)
    run(
        f"geneBody_coverage.py -i {bam_filenames_str} -r data/reference/{reference_downsampled} -o {qc_reports_directory}"
    )
    logger.info(
        "Finished rseqc gene body coverage analysis with outputs written to %s",
        qc_reports_directory,
    )


def generate_count_matrix(
    count_suffix: str,
    count_directory: str,
    filename_delimiter: str,
    output_filename: str,
):
    # identify all count files in the input directory
    count_files = glob(f"{count_directory}/*{count_suffix}")
    logger.info(
        f"Generating count matrix from {count_directory} N={len(count_files)} files"
    )
    counts = []
    for count_file in count_files:
        # read in the count file and set the appropriate columns
        df = pd.read_table(count_file, header=None)
        df.columns = ["GeneID", "Unstranded", "Sense-Stranded", "Antisense-Stranded"]
        df = df.iloc[4:][["GeneID", "Unstranded"]].set_index("GeneID")
        df.columns = [os.path.basename(count_file).split(filename_delimiter)[0]]
        counts.append(df)
    counts = pd.concat(counts, axis=1).fillna(0)
    os.makedirs(os.path.dirname(output_filename), exist_ok=True)
    df.to_csv(output_filename)
    logger.info(f"Count matrix generated at {output_filename}")


def run_multiqc(input_directory: str, output_directory: str) -> None:
    # make directory if it does not already exist
    os.makedirs(output_directory, exist_ok=True)
    # run MultiQC to summarize reports
    logger.info(f"Running MultiQC on {input_directory}")
    run(f"multiqc {input_directory} -o {output_directory}")
    logger.info(
        f"MultiQC completed successfully with outputs written to {output_directory}"
    )


def executor(pipeline_step: str, configs: Dict) -> Dict:
    # execute the pipeline step based on the given step name
    if pipeline_step == "qc_raw_fastq":
        run_fastqc(
            fastq_suffix=configs["fastq_suffix"],
            fastq_directory=configs["raw_fastq_directory"],
            output_directory=configs["raw_fastqc_directory"],
        )
    elif pipeline_step == "detect_adapters":
        r1_adapter, r2_adapter = detect_adapters(
            r1_fastq_suffix=configs["r1_fastqc_suffix"],
            r2_fastq_suffix=configs["r2_fastq_suffix"],
            fastq_suffix=configs["fastq_suffix"],
            fastq_directory=configs["raw_fastq_directory"],
            adapter_suffix=configs["adapter_suffix"],
            known_adapter_filename=configs["known_adapter_filename"],
            known_adapter_suffix=configs["known_adapter_suffix"],
            output_directory=configs["adapter_output_directory"],
        )
        # assign adapters to the configs for later steps
        configs["r1_adapter"] = r1_adapter
        configs["r2_adapter"] = r2_adapter
    elif pipeline_step == "trim_fastq":
        trim_fastqs(
            r1_fastq_suffix=configs["r1_fastq_suffix"],
            r2_fastq_suffix=configs["r2_fastq_suffix"],
            fastq_suffix=configs["fastq_suffix"],
            fastq_directory=configs["raw_fastq_directory"],
            r1_adapter=configs["r1_adapter"],
            r2_adapter=configs["r2_adapter"],
            trimmed_suffix=configs["trimmed_suffix"],
            trimmed_output_directory=configs["trimmed_fastq_directory"],
            qc_report_suffix=configs["cutadapt_output_suffix"],
            qc_reports_directory=configs["cutadapt_output_directory"],
        )
    elif pipeline_step == "qc_trimmed_fastq":
        run_fastqc(
            fastq_suffix=configs["trimmed_suffix"],
            fastq_directory=configs["trimmed_fastq_directory"],
            output_directory=configs["trimmed_fastqc_directory"],
        )
    elif pipeline_step == "map_fastq_to_bam":
        map_fastqs(
            r1_fastq_suffix=configs["r1_fastq_suffix"],
            r2_fastq_suffix=configs["r2_fastq_suffix"],
            fastq_directory=configs["trimmed_fastq_directory"],
            mapped_output_directory=configs["mapped_bam_directory"],
            reference_genome=configs["reference_genome"],
            n_cores=configs["n_cores"],
        )
    elif pipeline_step == "dedup_bam":
        dedup_bams(
            bam_directory=configs["mapped_bam_directory"],
            bam_suffix=configs["bam_suffix"],
            deduped_suffix=configs["deduped_suffix"],
            stats_suffix=configs["dedup_stats_suffix"],
            stats_directory=configs["dedup_stats_directory"],
        )
    elif pipeline_step == "qc_nondedup_bam":
        qc_mapped_data(
            bam_suffix=configs["bam_suffix"],
            bam_directory=configs["mapped_bam_directory"],
            qc_reports_directory=configs["bam_qc_reports_directory"],
            chr_stats_suffix=configs["chr_stats_suffix"],
            strand_inference_suffix=configs["strand_inference_suffix"],
            read_distribution_suffix=configs["read_distribution_suffix"],
            reference=configs["bam_qc_reference"],
            reference_downsampled=configs["bam_qc_reference_downsampled"],
        )
    elif pipeline_step == "aggregate_counts":
        generate_count_matrix(
            count_suffix=configs["count_suffix"],
            count_directory=configs["mapped_bam_directory"],
            filename_delimiter=configs["counts_filename_delimiter"],
            output_filename=configs["counts_output_filename"],
        )
    elif pipeline_step == "aggregate_qc_reports":
        run_multiqc(
            input_directory=configs["qc_reports_directory"],
            output_directory=configs["multiqc_output_directory"],
        )
    else:
        logger.error(f"Unknown pipeline step: {pipeline_step}")
        raise ValueError(f"Unknown pipeline step: {pipeline_step}")


def main():
    # read in command line arguments
    parser = argparse.ArgumentParser(description="Run Bulk RNA/ATAC/ChIP-Seq Pipeline")
    parser.add_argument(
        "-c",
        "--configuration_file",
        type=str,
        default="config.yaml",
        help="Path to the configuration file",
    )
    parser.add_argument(
        "-l",
        "--log_file",
        type=str,
        default="BulkPipeline.log",
        help="Path to the log file",
    )
    parser.add_argument(
        "-d",
        "--directory",
        type=str,
        default="./",
        help="Directory to run the pipeline in",
    )
    args = parser.parse_args()

    # configure logger and pipeline
    setup_logger(filename=args.log_file)
    configs = load_configs(filename=args.configuration_file)
    configs = configure_config(configs=configs)

    # identify where to begin the pipeline
    pipeline_steps = identify_start_step(configs=configs, pipeline_steps=PIPELINE_STEPS)

    # work through each step in the pipeline
    for step in pipeline_steps:
        logger.info(f"Executing pipeline step: {step}")
        try:
            executor(pipeline_step=step, configs=configs)
        except Exception as e:
            logger.error(f"Error in pipeline step {step}: {e}")
            raise ValueError(f"Error in pipeline step {step}: {e}")
    logger.info("Pipeline completed successfully!")


if __name__ == "__main__":
    main()
