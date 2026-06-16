# Задание 4. Визуализация в Yandex DataLens

## Цель

В рамках задания была выполнена визуализация результатов ETL- и Streaming-процессов в Yandex DataLens.

Для построения дашборда использовались результаты предыдущих этапов:

* batch processing из задания 2;
* Kafka streaming processing из задания 3.

Итоговый дашборд показывает аналитику по заявкам, решениям, рискам, продуктам и статусам документов.

## Использованные данные

### Batch dataset

Источник данных:

```text
Object Storage:
etl/task_02/output/applications/applications_cleaned/
```

Файл был скачан из Object Storage в формате Parquet и конвертирован в CSV для загрузки в DataLens через Files connection.

Итоговый локальный CSV:

```text
data/output/datalens/batch_applications_datalens.csv
```

Характеристики файла:

```text
rows=513486
columns=20
size=85.46 MB
```

Основные поля:

* `application_id`
* `event_time`
* `customer_id`
* `region_code`
* `product_type`
* `requested_amount`
* `term_months`
* `credit_score`
* `risk_level`
* `decision_status`
* `approved_amount`
* `channel`
* `processing_time_sec`
* `event_date`
* `is_approved`
* `is_rejected`
* `is_manual_review`

### Streaming dataset

Источник данных:

```text
Object Storage:
etl/task_03/output/loan_events_flattened/
```

Файл был скачан из Object Storage в формате Parquet и конвертирован в CSV для загрузки в DataLens через Files connection.

Итоговый локальный CSV:

```text
data/output/datalens/streaming_loan_events_datalens.csv
```

Характеристики файла:

```text
rows=173469
columns=17
size=29.42 MB
```

Основные поля:

* `application_id`
* `customer_id`
* `region_code`
* `loan_amount`
* `loan_term_months`
* `scoring_score`
* `scoring_risk_level`
* `document_type`
* `document_status`
* `documents_count`
* `decision_status`
* `submitted_at`
* `kafka_topic`
* `kafka_partition`
* `kafka_offset`
* `kafka_timestamp`
* `processed_at`

Особенность streaming dataset: одна заявка может занимать несколько строк, так как массив документов был разложен в плоскую структуру. Поэтому для подсчёта заявок использовалась агрегация `COUNTD(application_id)`, а для подсчёта документов — обычная агрегация `COUNT(application_id)`.

## Подготовка CSV-файлов

Так как DataLens Files connection принимает CSV-файлы, результаты PySpark в формате Parquet были предварительно конвертированы в CSV.

Для подготовки файлов был создан скрипт:

```text
scripts/prepare_datalens_csv.py
```

Запуск:

```powershell
python scripts\prepare_datalens_csv.py
```

Скрипт выполняет следующие действия:

1. читает Parquet part-файлы batch output;
2. объединяет их в один DataFrame;
3. сохраняет результат в CSV;
4. читает Parquet part-файл streaming output;
5. сохраняет streaming result в отдельный CSV.

Итоговые CSV-файлы использовались только для загрузки в DataLens и не предназначены для коммита в GitHub.

## DataLens objects

В DataLens был создан workbook:

```text
ETL Module 4
```

В workbook было создано подключение:

```text
etl_module_4_files_connection
```

Тип подключения:

```text
Files connection
```

В подключение были загружены два CSV-файла:

```text
batch_applications_datalens.csv
streaming_loan_events_datalens.csv
```

На основе подключения были созданы два датасета:

```text
ds_batch_applications
ds_streaming_loan_events
```

## Batch charts

На основе датасета `ds_batch_applications` были созданы следующие чарты.

### 1. Batch — Applications by Decision Status

Назначение: показать количество заявок по статусам решения.

Поля:

* измерение: `decision_status`
* метрика: `application_id`
* агрегация: количество уникальных

### 2. Batch — Applications by Day

Назначение: показать динамику количества заявок по дням.

Поля:

* измерение: `event_date`
* метрика: `application_id`
* агрегация: количество уникальных

### 3. Batch — Approval Rate by Risk Level

Назначение: сравнить долю одобренных заявок по уровням риска.

Поля:

* измерение: `risk_level`
* метрика: `approval_rate_pct`
* агрегация: среднее

Для отображения процента было создано вычисляемое поле:

```text
approval_rate_pct = is_approved * 100
```

### 4. Batch — Avg Requested Amount by Product

Назначение: сравнить среднюю запрашиваемую сумму по кредитным продуктам.

Поля:

* измерение: `product_type`
* метрика: `requested_amount`
* агрегация: среднее

## Streaming charts

На основе датасета `ds_streaming_loan_events` были созданы следующие чарты.

### 5. Streaming — Applications by Scoring Risk Level

Назначение: показать распределение уникальных заявок из Kafka по уровню скорингового риска.

Поля:

* измерение: `scoring_risk_level`
* метрика: `application_id`
* агрегация: количество уникальных

### 6. Streaming — Documents by Status

Назначение: показать количество документов по статусам проверки.

Поля:

* измерение: `document_status`
* метрика: `application_id`
* агрегация: количество

В этом чарте используется обычный подсчёт строк, так как каждая строка соответствует отдельному документу после flatten-преобразования JSON-событий Kafka.

## Dashboard

В DataLens был создан итоговый dashboard:

```text
ETL Module 4 — DataLens Dashboard
```

Структура дашборда:

```text
ETL Module 4 — Loan Applications Analytics

Batch:
- Applications by Decision Status
- Approval Rate by Risk Level
- Applications by Day
- Avg Requested Amount by Product

Kafka Streaming Analytics:
- Applications by Scoring Risk Level
- Documents by Status
```

Дашборд объединяет batch-аналитику и streaming-аналитику в одном представлении.

## Скриншоты

В папке `docs/images/task_04/` сохранены скриншоты выполнения задания 4:

* `1_datalens_files_connection_01.png`
* `1_datalens_files_connection_02.png`
* `2_datalens_batch_dataset_fields_01.png`
* `2_datalens_batch_dataset_fields_02.png`
* `3_datalens_streaming_dataset_fields.png`
* `4_datalens_dashboard_overview_01.png`
* `4_datalens_dashboard_overview_02.png`

## Результат

В результате выполнения задания 4:

1. результаты batch processing были подготовлены для визуализации;
2. результаты Kafka streaming были подготовлены для визуализации;
3. в DataLens было создано файловое подключение;
4. были созданы два датасета;
5. были построены 6 чартов;
6. был собран итоговый dashboard для визуализации результатов ETL- и Streaming-процессов.
