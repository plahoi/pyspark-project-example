Проект репозитория airflow дагов и pyspark скриптов.

Запуск:
```bash
spark-submit \
--master yarn \
--py-files packages.zip \
--files configs/config.json,configs/defaults.json \
jobs/calc_30/job.py
```

## Структура
### Configs
Папка содержит \*.json файлы с данными конфигурации или дефолтных параметров в виде json массивов. Данные файлы конфигурации передаются на все ноды с помощью `spark-submit --files configs/config.json,configs/defaults.json`

### Dags
Папка с дагами airflow в которой хранятся даги, каждый в своей папке.

### Dependencies
Папка содержит модули для работы в проекте. Для того чтобы данные модули были доступны на всех нодах spark кластера, они объединяются в массив packages.zip с помощью скрипта build и передаются с помощью `spark-submit --py-files packages.zip`

### Jobs
Содержит pyspark скрипты, каждый в своей папке.

#### build
Файл shell для сборки всех файлов в папке dependencies в файл packages. Запуск: `bash build`.
