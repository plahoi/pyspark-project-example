from pyspark.sql import Row
from pyspark.sql.functions import col, concat_ws, lit

from dependencies.spark import InitJob
import dependencies.utils as utils


def main():
    """Main ETL script definition.
    :return: None
    """
    # start Spark application and get Spark session, logger and config
    job = InitJob()
    spark = job.start_spark(app_name='my_etl_job')
    log = job.get_logger()
    config = job.get_config()
    defaults = job.get_defaults()

    # log that main ETL job is starting
    log.warn('etl_job is up-and-running')

    # execute ETL pipeline
    data = extract_data(spark)
    data_transformed = transform_data(data, defaults['infinite_date'])
    utils.test_persist_data(data_transformed)

    # log the success and terminate Spark application
    log.warn('test_etl_job is finished')
    spark.stop()
    return None

def extract_data(spark):
    path = 'jobs/test_apolyakov/data.csv'
    return spark.read.format("csv").option("header", "true").load(path)

def transform_data(data, date_to):
    return data.where(data.period_from_dt == date_to)

if __name__ == '__main__':
    main()