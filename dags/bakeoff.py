from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

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
    'freebayes': {
        'calling_opts': {},
        'filter_opts': {}
    },
    'common': {
        'reference': "/Users/mlyons/genomics/reference/human_g1k_v37.fasta"
    }
}
BAKE_OFF_PIPE = DAG(dag_id="bake_off", default_args=default_args, schedule_interval=timedelta(minutes=10))
WORK_DIR = "/tmp/" # make this unique per DAG instance so there aren't collisions

samtools_view = """samtools view {{ region }} > {{ region_bam }}"""
chromosome_split_operators = {}
for chromosome in range(1, 23):
    region = "chr" + str(chromosome)
    region_bam = "{WORK_DIR}/{region}.bam"
    chromosome_split_operators[region] = BashOperator(bash_command=samtools_view,
                                                      params=locals(),
                                                      dag=BAKE_OFF_PIPE,
                                                      task_id="{region}_split".format(**locals())
                                                      )


freebayes_command = """freebayes -f {{ reference }} --vcf {{ outfile }} --targets {{ region }} {{ opts }} {{ in_bam }}"""
freebayes_operators = {}
for toople in chromosome_split_operators.iteritems():
    region, operator = toople
    outfile = "{WORK_DIR}/{region}.vcf"
    freebayes_by_region = BashOperator(bash_command=freebayes_command,
                                       params={
                                           'reference': "/path/to/human.fasta",
                                           'outfile': outfile,
                                           'region': region,
                                           'opts': default_args['freebayes'],
                                           'in_bam': "{WORK_DIR}/{region}.bam".format(**locals())
                                       },
                                       dag=BAKE_OFF_PIPE,
                                       task_id="{region}_freebayes".format(**locals())
                                       )
    freebayes_operators[region] = freebayes_by_region
    freebayes_by_region.set_upstream(operator)


# now merge
vcf_concat_command = """vcf-concat-parts {{ in_files }} | vcf-sort > {{ outfile }}"""
infiles = []
for toople in freebayes_operators.iteritems():
    region, operator = toople
    infiles.append("{WORK_DIR}/{region}.vcf".format(**locals()))


concat_operator = BashOperator(bash_command=vcf_concat_command,
                               params={
                                   'in_files': ' '.join(infiles),
                                   'outfile': 'concatted.vcf'
                               },
                               task_id="vcf_concat",
                               dag=BAKE_OFF_PIPE,
                               wait_for_downstream=True)

# and dependencies
for operator in freebayes_operators.values():
    concat_operator.set_upstream(operator)



