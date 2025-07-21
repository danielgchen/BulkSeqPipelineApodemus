# pipeline steps, in order, from raw data to raw counts
PIPELINE_STEPS = [
    "qc_raw_fastq",
    "detect_adapters",
    "trim_fastq",
    "qc_trimmed_fastq",
    "map_fastq_to_bam" "dedup_bam",
    "qc_nondedup_bam",
    "aggregate_counts",
    "aggregate_qc_reports",
]
