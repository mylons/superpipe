from airflow.operators.bash_operator import BashOperator


SAM_TO_CRAM_COMMAND = """{{ params.bin }}/samtools view -T {{ params.reference }} -C -o {{ params.path_to_output_file }}/{{ params.output_file_name }}.cram {{ params.path_to_input_file }}"""

SAMTOOLS_FIXMATE_COMMAND = """{{ params.bin }}/samtools fixmate -O bam {{ params.path_to_input_file }} {{ params.path_to_output_file }}.bam"""

SAMTOOLS_COORDINATE_SORT_COMMAND = "{{ params.bin }}/samtools sort -O bam -o {{ params.path_to_output_file }}.sorted.bam -T {{ params.path_to_tmp_dir }} {{ params.path_to_input_file }}"
