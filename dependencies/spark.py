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
        application with the cluster. Note, that only the app_name argument
        will apply when this is called from a script sent to spark-submit.
        All other arguments exist solely for testing the script from within
        an interactive Python console.
        This function also looks for a file ending in 'config.json' that
        can be sent with the Spark job. If it is found, it is opened,
        the contents parsed (assuming it contains valid JSON for the ETL job
        configuration) into a dict of ETL job configuration parameters,
        which are returned as the last element in the tuple returned by
        this function. If the file cannot be found then the return tuple
        only contains the Spark session and Spark logger objects and None
        for config.
        The function checks the enclosing environment to see if it is being
        run from inside an interactive console session or from an
        environment which has a `DEBUG` environment variable set (e.g.
        setting `DEBUG=1` as an environment variable as part of a debug
        configuration within an IDE such as Visual Studio Code or PyCharm.
        In this scenario, the function uses all available function arguments
        to start a PySpark driver from the local PySpark package as opposed
        to using the spark-submit and Spark cluster defaults. This will also
        use local module imports, as opposed to those in the zip archive
        sent to spark via the --py-files flag in spark-submit.
        :param app_name: Name of Spark app.
        :param master: Cluster connection details (defaults to local[*]).
        :param jar_packages: List of Spark JAR package names.
        :param files: List of files to send to Spark cluster (master and
            workers).
        :param spark_config: Dictionary of config key-value pairs.
        :return: A tuple of references to the Spark session, logger and
            config dict (only if available).
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
        self.spark_logger = logging.Log4j(self.spark_session)
        return self.spark_logger

    def get_file(self, desired_filename):
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
