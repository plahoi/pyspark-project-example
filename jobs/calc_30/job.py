# -*- coding: utf-8 -*-
''' XXXX
'''
import datetime
import argparse
from collections import namedtuple
from pyspark.sql import SparkSession, HiveContext
from pyspark.conf import SparkConf
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, DateType

from dependencies.spark import InitJob
import dependencies.utils as utils

DEFAULT_DATE_FROM_DAYS = 365
DEFAULT_DATE_TO_DAYS = 1

class Job():
    def prepare_data(self):
        '''
        Извлечение данных из spark df в namedtuple по условию
        Attributes:
            src (dict): Словарь таблиц-источников (spark.table) данных
            date_from (string): Дата начала выборки
            date_to (string): Дата окончания выборки

        Returns:
            Data (namedtuple): Результат выборки данных по условию для каждой таблицы
        '''
        src = self.src
        infinite_date = self.defaults['infinite_date']

        Data = namedtuple('Data', 'psg, gds, pass_mgt, ticket_type')
        psg = src['psg_pass_ppr']
        gds = src['gds_goods']
        pass_mgt = src['pass_mgt']
        ticket_type = src['ticket_type']

        gds = F.broadcast(gds.where(
            F.col('day') == infinite_date).coalesce(1))
        psg = psg.where((F.col('day') >= self.date_from) & (
            F.col('day') <= self.date_to)).distinct()
        ticket_type = F.broadcast(ticket_type.where(
            F.col('day') == infinite_date).coalesce(1))
        pass_mgt = pass_mgt.where(
            (F.col('day') >= self.date_from) & (F.col('day') <= self.date_to)).distinct()

        return Data(psg, gds, pass_mgt, ticket_type)


    def rename_psg_columns(self, psg):
        psg = psg.withColumnRenamed('crd_serial', 'ticket_uid') \
            .withColumnRenamed('crd_no', 'ticket_num') \
            .withColumnRenamed('gd_id', 'ticket_type_id')
        return psg


    def join_data(self, Data):
        '''
        XXXX
        '''
        data = Data.psg.join(
            Data.gds,
            Data.psg.ticket_type_id == Data.gds.gd_id
        ).join(
            Data.ticket_type,
            Data.ticket_type.subway_ticket_type_id == Data.psg.ticket_type_id
        ).join(
            Data.pass_mgt,
            [
                Data.pass_mgt.ticket_code == Data.psg.ticket_num,
                Data.pass_mgt.ticket_type == Data.ticket_type.public_ticket_type_nm
            ]
        )

        return data


    def aggregate_and_collect_data(self, data):
        '''
        XXXX
        '''
        window = Window.partitionBy(
            data.hash_ticket_uid, data.ticket_num, data.ticket_type_id)

        data = data.select(
            data.hash_ticket_uid,
            data.ticket_num,
            data.ticket_type_id,

            data.name.alias('subway_ticket_type_nm'),
            data.public_ticket_type_nm,

            F.least(F.min(data.psg_date).over(window), F.min(
                data.pass_datetime).over(window)).cast(DateType()).alias('min_dt'),
            F.greatest(F.max(data.psg_date).over(window), F.max(
                data.pass_datetime).over(window)).cast(DateType()).alias('max_dt'),

            F.lit(None).cast(DateType()).alias('expire_dt'),

            # https://stackoverflow.com/a/45869254/3866209
            (F.size(F.collect_set(data.psg_date).over(window)) + \
                F.size(F.collect_set(data.pass_datetime).over(window))).alias('relation_cnt'),

            F.lit(1).alias('probability'),
            F.lit('TEST').alias('source'),

            F.lit(self.date_from).alias('period_from_dt'),
            F.lit(self.date_to).alias('period_to_dt'),
            F.current_timestamp().alias('process_dttm'),
            F.lit(self.date_to).cast(StringType()).alias('day')
        ).distinct()

        return data

    def calc(self, src, dst, **kwargs):
        ''' Composes all logic and modules
        '''
        self.src = utils.generate_spark_tables(src, self.spark)
        self.dst = dst

        self.date_from = kwargs.get(
            'date_from', (datetime.date.today() - datetime.timedelta(days=DEFAULT_DATE_FROM_DAYS)))
        self.date_to = kwargs.get(
            'date_to', (datetime.date.today() - datetime.timedelta(days=DEFAULT_DATE_TO_DAYS)))

        Data = self.prepare_data()
        Data = Data._replace(psg=self.rename_psg_columns(Data.psg))
        data = self.join_data(Data)
        data = self.aggregate_and_collect_data(data)
        utils.persist_result(data, self.dst)

        return None


    def main(self):
        ''' Declares default parameters and calls configured Spark context
        '''
        spark_config = {
            'spark.sql.shuffle.partitions': 1000,
            'spark.executor.cores': 6,
            'spark.executor.instances': 10,
            'spark.executor.memory': '8G',
            'spark.driver.cores': 6,
            'spark.driver.memory': '8G',
        }
        # start Spark application and get Spark session, logger and config
        job = InitJob()
        self.spark = job.start_spark(app_name='calc_30_tickets', spark_config=spark_config)
        self.log = job.get_logger()
        self.defaults = job.get_defaults()

        # log that main ETL job is starting
        self.log.warn('calc_30_tickets is up-and-running')

        src = {
            'psg_pass_ppr': 'psg_pass_ppr_hash_v_all',
            'gds_goods': 'gds_goods',
            'pass_mgt': 'pass_mgt',
            'ticket_type': 'd_ticket_type_map'
        }

        dst = 'dst_table'

        parser = argparse.ArgumentParser()
        parser.add_argument(
            '--date_from',
            action='store',
            type=utils.check_is_date_format,
            help='The Start Date - format YYYY-MM-DD. Default: today() - 365',
            required=False
        )
        parser.add_argument(
            '--date_to',
            type=utils.check_is_date_format,
            help='The End Date - format YYYY-MM-DD. Default: today() - 1',
            required=False
        )
        args = {key: value for key, value in vars(
            parser.parse_args()).items() if value}

        self.calc(src, dst, **args)

        # log the success and terminate Spark application
        self.log.warn('calc_30 is finished')
        self.spark.stop()
        return None


if __name__ == '__main__':
    job = Job()
    job.main()
