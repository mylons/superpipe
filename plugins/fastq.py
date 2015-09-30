import os
from airflow.operators.sensors import BaseSensorOperator
from airflow.plugins_manager import AirflowPlugin

import logging
import glob


class FastqSensor(BaseSensorOperator):

    def __init__(self,
                 directory,
                 poke_interval=60*1, # check for fastq files every x minutes
                 *args, **kwargs):
        super(FastqSensor, self).__init__(poke_interval=poke_interval, *args, **kwargs)

        self.fastq_directory = directory

        if not os.path.exists(self.fastq_directory):
            raise OSError("{self.fastq_directory} doesn't exist. need a valid directory with fastq files eventually put in it".format(**locals()))

    def execute(self, context):
        return super(FastqSensor, self).execute(context)

    def poke(self, context):
        logging.info('Looking in {self.fastq_directory}'.format(**locals()))
        directory_listing = glob.glob("{self.fastq_directory}/*.fastq".format(**locals()))
        try:
            # grab one
            the_file = directory_listing.pop()
            self.xcom_push(context, 'unmapped_fastq', the_file)
            logging.info("grabbed {the_file}".format(**locals()))
            return True
        except Exception as ex:
            logging.info("failed {ex}".format(**locals()))
            return False

# Defining the plugin class
class AirflowFastqPlugin(AirflowPlugin):
    name = "fastq_plugin"
    operators = [FastqSensor]

