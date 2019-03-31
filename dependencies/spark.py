"""
spark.py
~~~~~~~~
Module containing helper function for use with Apache Spark
"""

from os import listdir, path
import json
from pyspark import SparkFiles
from pyspark.sql import SparkSession, HiveContext

from dependencies import logging


class InitJob():
    def start_spark(self, app_name='default_pyspark_app', jar_packages=[],
                    files=['configs/config.json', 'configs/defaults.json'], spark_config={}, enable_hive=True):
        """Start Spark session, get Spark logger and load config files.
        Start a Spark session on the worker node and register the Spark
        application with the cluster.

        Arguments:
            app_name (string): Name of Spark app.
            master (Deprecated): Cluster connection details (defaults to local[*]).
            jar_packages (List): List of Spark JAR package names.
            files (List): List of files to send to Spark cluster (master and workers).
            spark_config (Dict): Dictionary of config key-value pairs.
        
        Returns: 
            Spark session
        """

        # get Spark session factory
        spark_builder = (
            SparkSession
            .builder
            .appName(app_name))
        # create Spark JAR packages string
        spark_jars_packages = ','.join(list(jar_packages))
        spark_builder.config('spark.jars.packages', spark_jars_packages)

        spark_files = ','.join(list(files))
        spark_builder.config('spark.files', spark_files)

        # add other config params
        for key, val in spark_config.items():
            spark_builder.config(key, val)

        # create session and retrieve Spark logger object
        if enable_hive:
            self.spark_session = spark_builder.enableHiveSupport().getOrCreate()
            hivecontext = HiveContext(self.spark_session.sparkContext)
            hivecontext.setConf('hive.exec.dynamic.partition', 'true')
            hivecontext.setConf(
                'hive.exec.dynamic.partition.mode', 'nonstrict')
        else:
            self.spark_session = spark_builder.getOrCreate()

        return self.spark_session

    def get_logger(self):
        ''' Returns the logger reference
        '''
        self.spark_logger = logging.Log4j(self.spark_session)
        return self.spark_logger

    def get_file(self, desired_filename):
        ''' This function looks for a file equals to 'desired_filename' that
        can be sent with the Spark job. If it is found, it is opened,
        the contents parsed (assuming it contains valid JSON for the ETL job
        configuration) into a dict of ETL job configuration parameters,
        which are returned as the last element in the tuple returned by
        this function. If the file cannot be found then the return tuple
        only contains the Spark session and Spark logger objects and None
        for config.

        Arguments:
            desired_filename (string): config filename assumed ***.json

        Returns:
            json like dict (Dict)
        '''
        # get config file if sent to cluster with --files
        spark_files_dir = SparkFiles.getRootDirectory()
        config_files = [filename
                        for filename in listdir(spark_files_dir)
                        if filename == desired_filename]

        if config_files:
            path_to_config_file = path.join(spark_files_dir, config_files[0])
            with open(path_to_config_file, 'r') as config_file:
                config_dict = json.load(config_file)
            self.spark_logger.warn(
                'loaded data from {}'.format(desired_filename))
        else:
            self.spark_logger.warn(
                'no {} file provided'.format(desired_filename))
            config_dict = None

        return config_dict

    def get_config(self, filename='config.json'):
        return self.get_file(filename)

    def get_defaults(self, filename='defaults.json'):
        return self.get_file(filename)
