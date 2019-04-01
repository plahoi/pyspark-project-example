Проект репозитория airflow дагов и pyspark скриптов.

spark-submit \
--master yarn \
--py-files packages.zip \
--files configs/config.json,configs/defaults.json \
jobs/calc_30/job.py