# Задание 2. Автоматизация работы с Yandex Data Processing при помощи Apache Airflow

## Цель

В рамках задания был реализован batch ETL-процесс для обработки CSV-файла с заявками на кредитные продукты. Обработка выполняется с помощью PySpark в Yandex Data Processing, а создание кластера, запуск задания и удаление кластера автоматизированы через Apache Airflow.

## Используемые сервисы

* Yandex Object Storage
* Yandex Managed Service for Apache Airflow
* Yandex Data Processing
* Yandex Virtual Private Cloud
* NAT Gateway
* Service Account

## Входные данные

В качестве входного файла используется:

```text
data/generated/applications.csv
```

Размер файла составляет около 60 MiB, что соответствует требованию задания о входном файле не менее 50 МБ.

Примерная структура файла:

```text
application_id,event_time,customer_id,region_code,product_type,requested_amount,term_months,credit_score,risk_level,decision_status,approved_amount,channel,employee_review_flag,processing_time_sec
```

Файл содержит синтетические данные о заявках клиентов на финансовые продукты.

## Структура Object Storage

Для задания использовался bucket:

```text
etl-transactions-v2-levon-20260615
```

Внутри bucket была подготовлена следующая структура:

```text
etl/task_02/input/applications.csv
etl/task_02/scripts/process_applications.py
etl/task_02/output/applications/
dags/etl_applications_dataproc_dag.py
```

Назначение директорий:

* `etl/task_02/input/` — входной CSV-файл.
* `etl/task_02/scripts/` — PySpark-скрипт обработки.
* `etl/task_02/output/` — результат работы PySpark job.
* `dags/` — DAG-файл Apache Airflow.

## PySpark-задание

PySpark-скрипт расположен в репозитории:

```text
spark/batch/process_applications.py
```

Скрипт выполняет следующие действия:

1. Читает CSV-файл `applications.csv` из Object Storage.
2. Применяет явную схему данных.
3. Преобразует поле `event_time` в timestamp/date.
4. Добавляет технические признаки:
   * `event_timestamp`
   * `event_date`
   * `event_month`
   * `is_approved`
   * `is_rejected`
   * `is_manual_review`
5. Сохраняет очищенный слой данных в Parquet.
6. Формирует аналитические агрегаты.
7. Записывает результаты обратно в Object Storage.

## Результирующие витрины

В результате выполнения PySpark job были сформированы следующие выходные наборы данных:

```text
etl/task_02/output/applications/applications_cleaned/
etl/task_02/output/applications/agg_by_region_product/
etl/task_02/output/applications/agg_by_day/
etl/task_02/output/applications/agg_by_channel/
```

### applications_cleaned

Очищенный детальный слой данных после преобразования типов и добавления технических колонок.

### agg_by_region_product

Агрегация по региону, продукту и уровню риска.

Основные метрики:

* количество заявок;
* количество одобренных заявок;
* количество отклонённых заявок;
* количество заявок на ручной проверке;
* approval rate;
* сумма запрошенных средств;
* сумма одобренных средств;
* средний credit score;
* среднее время обработки.

### agg_by_day

Дневная динамика заявок.

Основные метрики:

* количество заявок по дням;
* количество одобренных заявок;
* approval rate;
* сумма запрошенных средств;
* сумма одобренных средств;
* среднее время обработки.

### agg_by_channel

Агрегация по каналу подачи заявки и флагу ручной проверки.

Основные метрики:

* количество заявок;
* количество одобренных заявок;
* approval rate;
* средняя запрошенная сумма;
* средняя одобренная сумма;
* средний credit score;
* среднее время обработки.

## Airflow DAG

DAG-файл расположен в репозитории:

```text
airflow/dags/etl_applications_dataproc_dag.py
```

DAG выполняет три основные задачи:

```text
create_dataproc_cluster
        ↓
run_pyspark_applications_job
        ↓
delete_dataproc_cluster
```

### create_dataproc_cluster

Создаёт временный кластер Yandex Data Processing.

Используемое имя кластера:

```text
etl-applications-task-02
```

Кластер создаётся только на время выполнения обработки.

### run_pyspark_applications_job

Запускает PySpark job:

```text
s3a://etl-transactions-v2-levon-20260615/etl/task_02/scripts/process_applications.py
```

Аргументы задания:

```text
--input s3a://etl-transactions-v2-levon-20260615/etl/task_02/input/applications.csv
--output s3a://etl-transactions-v2-levon-20260615/etl/task_02/output/applications
```

### delete_dataproc_cluster

Удаляет временный кластер Yandex Data Processing после выполнения задания.

Для задачи удаления используется `TriggerRule.ALL_DONE`, чтобы кластер удалялся даже в случае ошибки на этапе PySpark job.

## Сетевые настройки

Для корректного создания Yandex Data Processing cluster была настроена маршрутизация через NAT Gateway.

Были созданы:

```text
etl-nat-gateway
etl-nat-route-table
```

В таблицу маршрутизации был добавлен маршрут:

```text
0.0.0.0/0 → etl-nat-gateway
```

Таблица маршрутизации была привязана к подсети:

```text
default-ru-central1-a
```

Это необходимо для того, чтобы кластер Yandex Data Processing мог обращаться к сервисам Yandex Cloud и Object Storage.

## Результат выполнения

DAG был успешно запущен в Apache Airflow.

Все задачи завершились успешно:

```text
create_dataproc_cluster          success
run_pyspark_applications_job     success
delete_dataproc_cluster          success
```

В логах PySpark job зафиксирован статус:

```text
status: DONE
name: process-applications-csv
```

После выполнения задания в Object Storage появились результирующие Parquet-датасеты.

## Скриншоты

Скриншоты выполнения задания сохранены в директории:

```text
docs/images/task_02/
```

Список скриншотов:

* 1_airflow_cluster_alive.png
* 2_airflow_dag_uploaded.png
* 3_pipeline_success_01.png
* 3_pipeline_success_02.png
* 4_object_storage_output.png

## Файлы в репозитории

Основные файлы, относящиеся к заданию 2:

```text
dataproc/README.md
airflow/dags/etl_applications_dataproc_dag.py
spark/batch/process_applications.py
data/generated/applications.csv
scripts/generate_applications.py
docs/images/task_02/
```

## Очистка ресурсов

Временный кластер Yandex Data Processing удаляется автоматически в рамках DAG задачей:

```text
delete_dataproc_cluster
```

После фиксации результатов и сохранения скриншотов также необходимо удалить временные платные ресурсы, если они больше не требуются:

* Managed Service for Apache Airflow cluster;
* NAT Gateway;
* route table для NAT;
* временные Data Processing ресурсы, если они остались.
