# Task 1. YDB → Object Storage через Yandex Data Transfer

В рамках задания был настроен перенос данных из Managed Service for YDB в Yandex Object Storage с помощью Yandex Data Transfer.

## Подготовленные данные

Для загрузки использовался файл:

```text
data/generated/transactions_v2.csv
```

Размер файла: около 35 МБ.

## YDB

В Yandex Cloud была создана serverless-база данных:

```text
etl-transactions-ydb
```

В базе данных была создана таблица:

```text
transactions_v2
```

Скрипт создания таблицы находится в репозитории:

```text
ydb/create_transactions_v2.yql
```

После загрузки CSV-файла в YDB была выполнена проверка данных с помощью скрипта:

```text
ydb/check_transactions_v2.yql
```

Результат проверки:

```text
rows_count = 343616
```

## Data Transfer

Для переноса данных были созданы:

```text
source endpoint: source-ydb-transactions-v2
target endpoint: target-object-storage-transactions-v2
transfer: transfer-ydb-to-object-storage-transactions-v2
```

Тип трансфера:

```text
Копирование
```

Трансфер завершился успешно.

## Object Storage

Для результата был создан bucket:

```text
etl-transactions-v2-levon-20260615
```

После выполнения трансфера в Object Storage появился CSV-файл:

```text
from_YDB/transactions_v2/part-*.csv
```

Размер выгруженного файла: около 35 МБ.

## Скриншоты

1. Созданная YDB database
2. Проверка загруженных данных в YDB
3. Завершённый Data Transfer
4. Результат в Object Storage

Подтверждающие скриншоты находятся в папке:

```text
docs/images/task_01/
```
