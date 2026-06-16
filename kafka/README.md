# Задание 3. Работа с топиками Apache Kafka и PySpark Streaming

## Цель

В рамках задания была реализована потоковая обработка JSON-сообщений из Apache Kafka с помощью PySpark-задания в Yandex Data Processing.

Цепочка обработки:

```text
JSONL file → Kafka Producer → Managed Kafka topic → PySpark Structured Streaming → Object Storage
```

## Используемые сервисы

* Yandex Managed Service for Apache Kafka
* Yandex Data Processing
* Yandex Object Storage
* Yandex Virtual Private Cloud

## Исходные данные

В качестве исходных данных использовался файл:

```text
data/generated/loan_events.jsonl
```

Файл содержит события по заявкам на кредиты в JSON-формате. Общий объём отправленных сообщений составил:

```text
24.93 MiB
```

Количество отправленных сообщений:

```text
69,367
```

Пример структуры JSON-сообщения:

```json
{
  "application_id": "loan_784512",
  "customer": {
    "customer_id": "cust_441",
    "region": "DE-HE"
  },
  "loan": {
    "amount": 15000,
    "term_months": 36
  },
  "scoring": {
    "score": 712,
    "risk_level": "medium"
  },
  "documents": [
    {
      "type": "passport",
      "status": "verified"
    }
  ],
  "decision_status": "manual_review",
  "submitted_at": "2026-05-01T10:15:11Z"
}
```

## Kafka

Был создан кластер Managed Service for Apache Kafka:

```text
etl-kafka-loan-events
```

Также был создан topic:

```text
loan-events
```

Параметры topic:

```text
Partitions: 1
Replication factor: 1
Cleanup policy: delete
```

Для подключения использовался пользователь Kafka:

```text
etl-user
```

Подключение выполнялось по протоколу:

```text
SASL_SSL
```

Порт подключения:

```text
9091
```

## Producer

Для отправки сообщений в Kafka был подготовлен скрипт:

```text
kafka/producer/produce_loan_events.py
```

Producer читает JSONL-файл построчно, проверяет корректность JSON, отправляет события в Kafka topic и выводит количество отправленных сообщений и общий объём payload.

Пример команды запуска:

```powershell
python kafka\producer\produce_loan_events.py `
  --bootstrap-servers <kafka-broker-fqdn>:9091 `
  --topic loan-events `
  --input data\generated\loan_events.jsonl `
  --username etl-user `
  --password <password> `
  --ssl-cafile certs\YandexInternalRootCA.crt
```

Результат отправки:

```text
Done. Sent 69,367 events to topic loan-events
Total payload size: 24.93 MiB
```

## PySpark Streaming job

Для обработки сообщений из Kafka был подготовлен PySpark Structured Streaming job:

```text
spark/streaming/kafka_loan_events_flatten.py
```

Скрипт выполняет следующие действия:

1. Подключается к Kafka topic `loan-events`.
2. Читает сообщения через `readStream`.
3. Преобразует поле `value` из Kafka в строку.
4. Парсит JSON через `from_json`.
5. Разворачивает вложенные структуры `customer`, `loan`, `scoring`.
6. Разворачивает массив `documents` через `explode_outer`.
7. Добавляет технические Kafka-поля: topic, partition, offset, timestamp.
8. Сохраняет результат в Object Storage в формате Parquet.
9. Использует checkpoint для Structured Streaming.

Для запуска использовался режим:

```python
trigger(once=True)
```

Это позволяет обработать доступные сообщения как streaming job и завершить задание со статусом `Done`.

## Плоская схема результата

После flatten результат сохраняется со следующими колонками:

```text
application_id
customer_id
region_code
loan_amount
loan_term_months
scoring_score
scoring_risk_level
document_type
document_status
documents_count
decision_status
submitted_at
kafka_topic
kafka_partition
kafka_offset
kafka_timestamp
processed_at
```

## Object Storage

PySpark-скрипт был загружен в Object Storage:

```text
s3a://etl-transactions-v2-levon-20260615/etl/task_03/scripts/kafka_loan_events_flatten.py
```

Результат обработки сохранён в:

```text
s3a://etl-transactions-v2-levon-20260615/etl/task_03/output/loan_events_flattened
```

Checkpoint сохранён в:

```text
s3a://etl-transactions-v2-levon-20260615/etl/task_03/checkpoints/loan_events_flatten_v1
```

В output-папке был создан Parquet-файл:

```text
part-00000-...snappy.parquet
```

Также была создана служебная папка:

```text
_spark_metadata
```

## Скриншоты

Скриншоты выполнения задания сохранены в директории:

```text
docs/images/task_03/
```

Список скриншотов:

* 1_kafka_security_group_01.png
* 1_kafka_security_group_02.png
* 1_kafka_security_group_03.png
* 2_kafka_cluster_alive.png
* 3_kafka_topic_created.png
* 4_kafka_user_created.png
* 5_kafka_producer_sent_25mb.png
* 6_spark_script_uploaded_to_object_storage.png
* 7_dataproc_cluster_alive.png
* 8_dataproc_pyspark_job_done_01.png
* 8_dataproc_pyspark_job_done_02.png
* 9_object_storage_streaming_output.png
* 10_streaming_checkpoint.png

## Результат

В результате была реализована потоковая обработка данных:

```text
Kafka topic → PySpark Structured Streaming → Flatten JSON → Object Storage Parquet
```

Требование по объёму переданных данных выполнено: `24.93 MiB > 20 MiB`.

PySpark-задание успешно завершилось со статусом `Done`.

Результат сохранён в Object Storage в формате Parquet.
