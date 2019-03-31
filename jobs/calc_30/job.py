# -*- coding: utf-8 -*-
''' Связка мастер-атрибутов пассажира Московского ОТ или автомобилиста Москвы:
    Хэш идентификатора чипа билета Метро + Тип билета НГПТ + Номер билета НГПТ
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
        psg = psg.withColumnRenamed('crd_serial_no_hash', 'hash_ticket_uid') \
            .withColumnRenamed('crd_no', 'ticket_num') \
            .withColumnRenamed('gd_id', 'ticket_type_id')
        return psg


    def join_data(self, Data):
        '''
        Соединение таблиц для реализации пунктов 4.2.4, 4.2.5, 4.2.6, 4.2.7, 4.2.9
        (приведено описание без учета переименования колонок в rename_psg_columns())

        metro_data.gds_goods.gd_id = metro_data.psg_pass_ppr_hash_v_all.gd_id

        dict.d_ticket_type_map.subway_ticket_type_id = metro_data.psg_pass_ppr_hash_v_all.gd_id

        mosgortr_data.pass_mgt.ticket_code = ticket_num из metro_data.psg_pass_ppr_hash_v_all
            и mosgortr_data.pass_mgt.ticket_type = public_ticket_type_nm из dict.d_ticket_type_map

        mosgortr_data.pass_mgt.ticket_code = ticket_num из metro_data.psg_pass_ppr_hash_v_all
            и mosgortr_data.pass_mgt.ticket_type = public_ticket_type_nm из dict.d_ticket_type_map

        mosgortr_data.pass_mgt.ticket_code = ticket_num из metro_data.psg_pass_ppr_hash_v_all
            и mosgortr_data.pass_mgt.ticket_type = public_ticket_type_nm из dict.d_ticket_type_map
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
        4.2.1. Атрибут HASH_TICKET_UID заполняем значением из METRO_DATA.PSG_PASS_PPR_HASH_V_ALL.CRD_SERIAL_NO_HASH
        4.2.2. Атрибут TICKET_NUM заполняем значением из METRO_DATA.PSG_PASS_PPR_HASH_V_ALL.CRD_NO
        4.2.3. Атрибут TICKET_TYPE_ID заполняем значением из METRO_DATA.PSG_PASS_PPR_HASH_V_ALL.GD_ID
        4.2.4. Атрибут SUBWAY_TICKET_TYPE_NM заполняем значением из METRO_DATA.GDS_GOODS.NAME
        4.2.5. Атрибут PUBLIC_TICKET_TYPE_NM заполняем из DICT.D_TICKET_TYPE_MAP.PUBLIC_TICKET_TYPE_NM
        4.2.6. Атрибут MIN_DT заполняем минимальным значением даты без времени из
            METRO_DATA.PSG_PASS_PPR_HASH_V_ALL.PSG_DATE, MOSGORTR_DATA.PASS_MGT.PASS_DATETIME
        4.2.7. Атрибут MAX_DT заполняем максимальным значением даты без времени из
            METRO_DATA.PSG_PASS_PPR_HASH_V_ALL.PSG_DATE, MOSGORTR_DATA.PASS_MGT.PASS_DATETIME
        4.2.8. Атрибут EXPIRE_DT заполняем значением null
        4.2.9. Атрибут RELATION_CNT заполняем количеством уникальных записей по
            METRO_DATA.PSG_PASS_PPR_HASH_V_ALL.PSG_DATE, MOSGORTR_DATA.PASS_MGT.PASS_DATETIME
        4.2.10. Атрибут PROBABILITY заполняем значением 1
        4.2.11. Атрибут SOURCE заполняем значением 'METRO_AND_MOSGORTR'
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
            F.lit('METRO_AND_MOSGORTR').alias('source'),

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
            'psg_pass_ppr': 'metro_data.psg_pass_ppr_hash_v_all',
            'gds_goods': 'metro_data.gds_goods',
            'pass_mgt': 'mosgortr_data.pass_mgt',
            'ticket_type': 'dict.d_ticket_type_map'
        }

        dst = 'sandbox_dev.case_profile_calc_d_subway_ticket_x_public_ticket'

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
        self.log.warn('calc_30_tickets is finished')
        self.spark.stop()
        return None


if __name__ == '__main__':
    job = Job()
    job.main()
