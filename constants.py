# pipeline steps, in order, from raw data to raw counts
PIPELINE_STEPS = [
    "qc_raw_fastq",
    "detect_adapters",
    "quantify_adapters",
    "trim_fastq",
    "qc_trimmed_fastq",
    "map_fastq_to_bam",
    "index_bam",
    "dedup_bam",
    "index_dedup_bam",
    "qc_nondedup_bam",
    "aggregate_counts",
    "aggregate_qc_reports",
]
# quality control directories
RUN_DIRECTORIES = [
    "raw_fastqc_directory",
    "trimmed_fastq_directory",
    "trimmed_fastqc_directory",
    "adapter_output_directory",
    "cutadapt_output_directory",
    "mapped_bam_directory",
    "dedup_stats_directory",
    "bam_qc_reports_directory",
    "counts_output_directory",
    "qc_reports_directory",
    "multiqc_output_directory",
]
# location of the status file
STATUS_FILE = "status.log"
