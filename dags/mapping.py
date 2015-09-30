"""
from hugeseq
samtools bam2fq $bam | bwa mem -CMp $optT $optRG $REF - | samtools view -Sbt $REF.fai -o ${bam/.bam/}.bwa.bam -

maybe use PythonOperator for moving files around?
use S3FileTransformOperator in an s3 scenario
"""
from airflow.operators.bash_operator import BashOperator

BWA_MEM_COMMAND = """{{ params.bin }}/bwa mem {{ params.path_to_reference_file }} {{ ti.xcom_pull(key='unmapped_fastq', task_ids="fastq_sensor") }} > {{ params.path_to_output }}/{{ task_instance_key_str }}.sam"""


