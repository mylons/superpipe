"""
The Simple Pipe:
    Assumptions:
        Not paired data
        Input is fastq (just to add more complexity in the upfront conversion to sam/bam and maybe cram)
        Reference is already indexed by bwa (could add a task to do this, but no point for a demo)
    Design
        1: Sensor to watch for a fastq file to show up somewhere
        2: Data xfer to cluster for analysis
        3:

Ideas:
    - Use drmaa hooks to launch jobs on cluster as Tasks in graph
        - Consider relying on airflow pools instead for resource management
    - use ExternalTaskSensor for inter DAG task spawning
"""
from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from mapping import BWA_MEM_COMMAND
from airflow.operators import FastqSensor

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}
THE_HUMAN_GENOME = "/Users/mlyons/genomics/reference/human_g1k_v37.fasta"
BAM_DIR = "/Users/mlyons/genomics/1kg/bam"
BIN_DIR = "/Users/mlyons/genomics/bin"

simple_mapping_pipeline = DAG(dag_id="simple_mapping_pipeline", default_args=default_args, schedule_interval=timedelta(minutes=2))

# figure out some sensor to look for a fastq file to map
fastq_sensor = FastqSensor(directory="/Users/mlyons/genomics/1kg/unprocessed_fastq",
                           dag=simple_mapping_pipeline,
                           task_id='fastq_sensor',
                           poke_interval=60)

"""bwa mem {{ path_to_reference_file }} {{ ti.xcom_pull('unmapped_fastq') }} > {{ path_to_output }}/{{ task_instance_key_str }}.sam"""
bwa_mem = BashOperator(bash_command=BWA_MEM_COMMAND,
                       params={'path_to_reference_file': THE_HUMAN_GENOME,
                               'path_to_output': BAM_DIR,
                               'bin': BIN_DIR},
                       dag=simple_mapping_pipeline,
                       task_id='bwa_mem',
                       wait_for_downstream=False)

bwa_mem.set_upstream(fastq_sensor)


