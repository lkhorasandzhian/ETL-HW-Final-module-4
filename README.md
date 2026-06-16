# ETL Yandex Cloud Data Platform HW Final

Итоговое домашнее задание (модуль 4) по учебной дисциплине «ETL-процессы».  
Тема: «реализация ETL-процесса».  
Выполнил студент: **Хорасанджян Левон, МИНДА251**.

В рамках проекта реализован полный ETL/Streaming pipeline в Yandex Cloud:

1. перенос данных из YDB в Object Storage через Yandex DataTransfer;
2. batch-обработка данных через Apache Airflow, Yandex Data Processing и PySpark;
3. streaming-обработка Kafka topic через PySpark;
4. визуализация результатов в Yandex DataLens.

## Навигация по отчётам

Подробные отчёты по каждому заданию вынесены в отдельные README-файлы.

| | Описание                                                   | Отчёт                                                                                             |
| ---------------- | ------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------ |
| Задание 1 | YDB → Object Storage через Yandex DataTransfer               | [ydb/README.md](https://github.com/lkhorasandzhian/ETL-HW-Final-module-4/blob/main/ydb/README.md)           |
| Задание 2 | Apache Airflow + Yandex Data Processing + PySpark batch processing | [dataproc/README.md](https://github.com/lkhorasandzhian/ETL-HW-Final-module-4/blob/main/dataproc/README.md) |
| Задание 3 | Kafka topic + PySpark Streaming + flatten JSON                     | [kafka/README.md](https://github.com/lkhorasandzhian/ETL-HW-Final-module-4/blob/main/kafka/README.md)       |
| Задание 4 | Yandex DataLens dashboard                                          | [datalens/README.md](https://github.com/lkhorasandzhian/ETL-HW-Final-module-4/blob/main/datalens/README.md) |

## Структура проекта

<details>
<summary>Посмотреть / Скрыть</summary>

<pre>
ETL-HW-Final-module-4/
├── airflow/
│   └── dags/
│       └── etl_applications_dataproc_dag.py
├── data/
│   └── samples/
│       ├── .gitkeep
│       └── applications_sample.csv
├── datalens/
│   └── README.md
├── dataproc/
│   └── README.md
├── docs/
│   └── images/
│       ├── task_01/
│       │   ├── 1_etl_transactions_ydb.png
│       │   ├── 2_ydb_fulfill_01.png
│       │   ├── 2_ydb_fulfill_02.png
│       │   ├── 3_transfer.png
│       │   └── 4_object_storage_result.png
│       ├── task_02/
│       │   ├── 1_airflow_cluster_alive.png
│       │   ├── 2_airflow_dag_uploaded.png
│       │   ├── 3_pipeline_success_01.png
│       │   ├── 3_pipeline_success_02.png
│       │   └── 4_object_storage_output.png
│       ├── task_03/
│       │   ├── 1_kafka_security_group_01.png
│       │   ├── 1_kafka_security_group_02.png
│       │   ├── 1_kafka_security_group_03.png
│       │   ├── 2_kafka_cluster_alive.png
│       │   ├── 3_kafka_topic_created.png
│       │   ├── 4_kafka_user_created.png
│       │   ├── 5_kafka_producer_sent_25mb.png
│       │   ├── 6_spark_script_uploaded_to_object_storage.png
│       │   ├── 7_dataproc_cluster_alive.png
│       │   ├── 8_dataproc_pyspark_job_done_01.png
│       │   ├── 8_dataproc_pyspark_job_done_02.png
│       │   ├── 9_object_storage_streaming_output.png
│       │   └── 10_streaming_checkpoint.png
│       └── task_04/
│           ├── 1_datalens_files_connection_01.png
│           ├── 1_datalens_files_connection_02.png
│           ├── 2_datalens_batch_dataset_fields_01.png
│           ├── 2_datalens_batch_dataset_fields_02.png
│           ├── 3_datalens_streaming_dataset_fields.png
│           ├── 4_datalens_dashboard_overview_01.png
│           └── 4_datalens_dashboard_overview_02.png
├── kafka/
│   ├── producer/
│   │   └── produce_loan_events.py
│   └── README.md
├── scripts/
│   ├── generate_all_data.py
│   ├── generate_applications.py
│   ├── generate_loan_events_jsonl.py
│   ├── generate_transactions_v2.py
│   └── prepare_datalens_csv.py
├── spark/
│   ├── batch/
│   │   └── process_applications.py
│   └── streaming/
│       └── kafka_loan_events_flatten.py
├── ydb/
│   ├── check_transactions_v2.yql
│   ├── create_transactions_v2.yql
│   └── README.md
├── .gitignore
├── LICENSE
├── README.md
└── requirements.txt
</pre>

</details>

## Основные артефакты проекта

- airflow/dags/etl_applications_dataproc_dag.py
- spark/batch/process_applications.py
- spark/streaming/kafka_loan_events_flatten.py
- kafka/producer/produce_loan_events.py
- scripts/generate_all_data.py
- scripts/prepare_datalens_csv.py
- ydb/create_transactions_v2.yql
- ydb/check_transactions_v2.yql

## Данные

Для проверки заданий были подготовлены синтетические данные:

| Файл                | Назначение               | Объём |
| ----------------------- | ---------------------------------- | ---------: |
| `transactions_v2.csv` | задание 1, YDB/DataTransfer |      35 MB |
| `applications.csv`    | задание 2, batch processing |      60 MB |
| `loan_events.jsonl`   | задание 3, Kafka streaming  |      25 MB |

Файлы генерируются локально скриптами из папки `scripts/` и не предназначены для хранения в репозитории как основные артефакты.

Запуск генерации:

```powershell
python scripts/generate_all_data.py
```

## Скриншоты

Скриншоты выполнения заданий сохранены в папке `docs/images/`.

## DataLens

Для задания 4 был создан итоговый dashboard.

Он визуализирует результаты batch- и streaming-обработки:

* распределение заявок по статусам решений;
* динамику заявок по дням;
* approval rate по уровням риска;
* среднюю сумму заявки по продуктам;
* распределение Kafka-заявок по уровню скорингового риска;
* количество документов по статусам проверки.

## Финальное состояние инфраструктуры

После выполнения и проверки заданий дорогие временные ресурсы были удалены:

* Yandex Data Processing clusters;
* Managed Service for Apache Kafka cluster;
* временные сетевые ресурсы для Kafka/Data Processing.

Оставлены только необходимые базовые ресурсы, такие как Object Storage, Service Account, VPC и Cloud Logging.

## Итог

В результате выполнены все 4 задания итогового проекта: DataTransfer, Airflow/Data Processing, Kafka/PySpark Streaming и DataLens. Подробные отчёты и скриншоты находятся в отдельных README-файлах и папке `docs/images/`.
