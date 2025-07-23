## Ol' Reliable Apodemus - a standard bulk-seq pipeline

---
### Quick Start

---

#### Install Relevant Packages
The following code is meant to be run in the command line interface (CLI) of the Fred Hutch scientific computing server. Please adjust to your own server details as needed, i.e. install Python natively and installing FastQC etc. from their relevant websites. You will need to make sure to have RSeqQC, Flask, PyYAML, and Picard installed separately and one method of attaching them is shown below, e.g. by setting the $PICARD variable.
* module load Python/3.7.4-foss-2019b-fh1
* module load FastQC/0.11.9-Java-11
* module load MultiQC/1.9-foss-2019b-Python-3.7.4
* module load cutadapt/2.9-foss-2019b-Python-3.7.4
* module load BBMap/38.91-GCC-10.2.0
* module load STAR/2.7.7a-GCC-10.2.0
* module load picard/2.25.0-Java-11
* module load SAMtools/1.11-GCC-10.2.0
* module load R/4.4.2-gfbf-2024a
* export PICARD=$EBROOTPICARD/picard.jar
* pip install RSeQC Flask PyYAML

---

#### Adjust Pipeline Configurations
See the `example_inputs/configs.yaml` file for an example of what is required for the pipeline. Most parameters values will not need to be changed. Please see the following for the main parameters that will require customization.
| Parameter | Example | Description |
| -------- | ------- | ------- |
| `pipeline_start_step` | _"qc_raw_fastq"_ | The step to start the pipeline from, e.g. you may start directly from BAM files. |
| `run_directory` | _"./"_ | Where the program may output files and find intermediate files. |
| `raw_fastq_directory` | _"./raw_fastqs"_ | Directory with raw fastq files. |
| `fastq_suffix` | _".fastq.gz"_ | Suffix used to find FASTQ files, e.g. also ".fq.gz" |
| `r1`, `r2` | _"read1"_, _"read2"_ | How the forward (read1) and reverse (read2) are called. These should be right before your `fastq_suffix`. |
| `reference_genome` | _"./hg38_STAR"_ | Location of a STAR indexed reference genome to map reads to. |
| `n_cores` | _10_ | Number of cores the program should utilize, more is faster but more resource intensive. |
| `bam_qc_reference` | _"./hg38_genes.bed"_ | BED formatted files of genes to utilized for BAM QC. |
| `bam_qc_reference_downsampled` | _"./hg38_genes_2k.bed"_ | Downsampled version of the above for gene body coverage analysis. |

---

#### Run the Pipeline
Pipeline can then be run from the command line utilizing `python main.py -c <CONFIGURATION_FILE>`. This is via the CLI, you could also run this via a graphical-user-interface, by editing your own configuration file and opening a Flask app via `cd gui` to enter the GUI directory and then `python app.py` which will provide you a link to open a website able to run the pipeline for you and track the current pipeline status.
