# ETL Module 4 Exam: Yandex Cloud Data Platform

Итоговое практическое задание 4-го модуля по дисциплине «ETL-процессы».

## 1. Цель работы

Реализовать набор ETL/Streaming-процессов в Yandex Cloud:

1. Перенос данных из Managed Service for YDB в Object Storage через Yandex DataTransfer.
2. Автоматизация обработки файлов 50+ МБ через Apache Airflow и Yandex Data Processing / PySpark.
3. Чтение Kafka topic через PySpark, разбор вложенного JSON в плоский вид, объём передачи 20+ МБ.
4. Построение аналитических дашбордов в Yandex DataLens.

## 2. Структура репозитория

```text
ETL-HW-Final-module-4/
├── README.md
├── requirements.txt
├── scripts/
│   ├── generate_transactions_v2.py
│   ├── generate_applications.py
│   ├── generate_loan_events_jsonl.py
│   └── generate_all_data.py
├── ydb/
│   ├── create_transactions_v2.yql
│   ├── check_transactions_v2.yql
│   └── README.md
├── airflow/
│   └── dags/
│       └── etl_applications_dataproc_dag.py
├── spark/
│   ├── batch/
│   │   └── process_applications.py
│   └── streaming/
│       └── kafka_loan_events_flatten.py
├── kafka/
│   └── producer/
│       └── produce_loan_events.py
├── datalens/
│   └── README.md
├── docs/
│   └── images/
└── data/
    ├── raw/
    ├── generated/
    └── samples/
```

## 3. Подготовка синтетических данных

Сгенерированные большие файлы не коммитятся в Git, они создаются локально и загружаются в Object Storage/YDB/Kafka.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

python scripts/generate_all_data.py

ls -lh data/generated/
```

Ожидаемые файлы:

| Файл                | Назначение            | Требуемый объём | Объём по умолчанию |
| ----------------------- | ------------------------------- | ----------------------------: | ---------------------------------: |
| `transactions_v2.csv` | Task 1, YDB/DataTransfer        |                      30+ МБ |                             35 MiB |
| `applications.csv`    | Task 2, Airflow/Data Processing |                      50+ МБ |                             60 MiB |
| `loan_events.jsonl`   | Task 3, Kafka/PySpark           |                      20+ МБ |                             25 MiB |

## 4. Задание 1. Yandex DataTransfer: YDB → Object Storage

### 4.1. Что сделано

- Создана YDB database: `<указать имя/ID>`.
- Создана таблица `transactions_v2` с помощью `ydb/create_transactions_v2.yql`.
- Загружены данные из `transactions_v2.csv`.
- Создан бакет Object Storage: `<указать bucket>`.
- Настроен transfer из YDB в Object Storage.
- Проверена выгрузка файлов в Object Storage.

### 4.2. Артефакты

- YQL-скрипт создания таблицы: `ydb/create_transactions_v2.yql`.
- YQL-скрипт проверок: `ydb/check_transactions_v2.yql`.
- Скриншоты: `docs/images/task1_*.png`.

## 5. Задание 2. Apache Airflow + Yandex Data Processing

### 5.1. Что сделано

- Подготовлен входной файл `applications.csv` объёмом 50+ МБ.
- Файл загружен в Object Storage: `s3://<bucket>/raw/applications/applications.csv`.
- Подготовлено PySpark-задание: `spark/batch/process_applications.py`.
- Подготовлен DAG: `airflow/dags/etl_applications_dataproc_dag.py`.
- DAG создаёт кластер Yandex Data Processing, запускает PySpark job и удаляет кластер.
- Результат записывается в Object Storage: `s3://<bucket>/processed/applications_agg/`.

### 5.2. Результирующая витрина

Гранулярность: дата, регион, продукт, риск, статус решения.

Поля:

- `event_date`
- `region_code`
- `product_type`
- `risk_level`
- `decision_status`
- `applications_count`
- `requested_amount_total`
- `approved_amount_total`
- `avg_credit_score`
- `avg_processing_time_sec`

## 6. Задание 3. Kafka + PySpark

### 6.1. Что сделано

- Подготовлен файл `loan_events.jsonl` объёмом 20+ МБ.
- Создан Kafka topic: `<topic name>`.
- События отправлены в Kafka producer-скриптом `kafka/producer/produce_loan_events.py`.
- PySpark job `spark/streaming/kafka_loan_events_flatten.py` читает topic, разбирает JSON и сохраняет плоскую таблицу.

### 6.2. Плоская структура результата

- `application_id`
- `customer_id`
- `region_code`
- `amount`
- `term_months`
- `score`
- `risk_level`
- `decision_status`
- `submitted_at`
- `documents_count`
- `has_rejected_document`

## 7. Задание 4. DataLens

Построены дашборды по результатам загрузки и обработки данных.

### 7.1. Дашборд 1: Applications

- Количество заявок.
- Approval rate.
- Сумма запрошенных и одобренных кредитов.
- Распределение по регионам, продуктам и уровню риска.

### 7.2. Дашборд 2: Kafka Loan Events

- Количество событий по времени.
- Доля manual review.
- Доля заявок с отклонёнными документами.
- Сумма заявок по risk level.

### 7.3. Дашборд 3: Calls / DataTransfer

- Количество звонков по регионам.
- Распределение по campaign type.
- Follow-up required.

Скриншоты дашбордов: `docs/images/task4_*.png`.

## 8. Проверка результатов

Добавить сюда команды, скриншоты и короткое описание проверок:

```bash
# Размеры локально сгенерированных файлов
ls -lh data/generated/

# Пример локального запуска batch Spark job
spark-submit spark/batch/process_applications.py \
  --input data/generated/applications.csv \
  --output data/output/applications_agg
```

## 9. Что важно остановить после проверки

Чтобы не расходовать ресурсы облака, после проверки были остановлены/удалены:

- временный кластер Yandex Data Processing;
- лишние Airflow/Kafka ресурсы, если они больше не нужны;
- временные endpoints/transfers, если они использовались только для проверки.

## 10. Итог

В результате работы подготовлен полный ETL/Streaming pipeline с переносом данных через DataTransfer, batch-обработкой в PySpark, streaming-обработкой Kafka topic и визуализацией в DataLens.
