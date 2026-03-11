Используем модель "звезда", так как для небольшого проекта использование DataVault или DataVault 2.0 не имеет смысла ввиду сложного создания модели.
Модель "снежинка" уступает "звезде" так как для данного проекта не нужна особая нормализация данных. Количество различных полей в исходных таблицах мало, поэтому систематизация данных как в "снежинке" не нужна, хватает первого уровня вложенности таблиц измерений.
### Нужна ли нормализация/денормализация
Так как для проекта скорость выполнения запросов не особо важна, и данных в полученных таблицах несильно много, то полная денормализация не нужна. Но при полной нормализации запросы становятся более сложными в написании (приходится писать много join'ов).

Поэтому стоит нормализовать следующие данные:
- Редко изменяемые данные, такие как список локаций, список типов оплаты и так далее (для большей скорости обработки запросов такие данные лучше хранить в словарях)

Денормализовать:
- Часто изменяемые данные, такие как дата
- Часто используемые поля для фильтрации, для GROUP BY, например такие метрики, как passenger_count, total_amount

### Партиционирование
Так как данные в таблице хранятся за один месяц, то логичным будет разделить ее на партиции по дням. В одной партиции будет храниться около 150к строк, что приемлемо для пет-проекта. При этом партиций будет около 30 - что тоже оптимально. Также стоит отметить, что благодаря такому партиционированию добавиться небольшая оптимизация запросов (небольшая так как запросов по дням при аналитике происходит не особо много).

### Архитектура "звезды"

Спроектируем модель на основе выводов, полученных выше:

**fact_table**:
- taxi_type_id
- vendor_id
- ratecode_id
- PU_location_id
- DO_location_id
- payment_id
- pickup_datetime
- pickup_day
- pickup_hour
- dropoff_datetime
- dropoff_day
- dropoff_hour
- passenger_count
- trip_distance
- fare_amount
- extra
- mta_tax
- tip_amount
- tolls_amount
- improvement_surcharge
- total_amount
- congestion_surcharge
- airport_fee
- cbd_congestion_fee

**taxi_type_dim**:
- taxi_type_id
- taxi_type_name

**vendor_dim**:
- vendor_id
- name

**payment_dim**:
- payment_id
- payment_type

**ratecode_dim**:
- ratecode_id
- codename

**location_dim**:
- PU_location_id
- borough
- zone
- service_zone

Создание схемы исходных таблиц в Clickhouse с помощью подключения к PostgreSQL (эти таблицы физически не хранятся в клике).
```sql
CREATE DATABASE taxi_dwh;

CREATE TABLE taxi_dwh.src_ylw_trips
(
	VendorID UInt8,
	tpep_pickup_datetime DateTime,
	tpep_dropoff_datetime DateTime,
	passenger_count UInt8,
	trip_distance Float32,
	RatecodeID UInt8,
	store_and_fwd_flag String,
	PULocationID UInt16,
	DOLocationID UInt16,
	payment_type UInt8,
	fare_amount Decimal(10,2),
	extra Decimal(10,2),
	mta_tax Decimal(10,2),
	tip_amount Decimal(10,2),
	tolls_amount Decimal(10,2),
	improvement_surcharge Decimal(10,2),
	total_amount Decimal(10,2),
	congestion_surcharge Decimal(10,2),
	Airport_fee Decimal(10,2),
	cbd_congestion_fee Decimal(10,2)
)
ENGINE = PostgreSQL('postgres:5432', 'airflow', 'yellow_tripdata', 'airflow', 'airflow');

CREATE TABLE taxi_dwh.src_grn_trips
(
	VendorID UInt8,
	lpep_pickup_datetime DateTime,
	lpep_dropoff_datetime DateTime,
	store_and_fwd_flag String,
	RatecodeID UInt8,
	PULocationID UInt16,
	DOLocationID UInt16,
	passenger_count UInt8,
	trip_distance Float32,
	fare_amount Decimal(10,2),
	extra Decimal(10,2),
	mta_tax Decimal(10,2),
	tip_amount Decimal(10,2),
	tolls_amount Decimal(10,2),
	ehail_fee String,
	improvement_surcharge Decimal(10,2),
	total_amount Decimal(10,2),
	payment_type UInt8,
	trip_type UInt8,
	congestion_surcharge Decimal(10,2),
	cbd_congestion_fee Decimal(10,2)
)
ENGINE = PostgreSQL('postgres:5432', 'airflow', 'green_tripdata', 'airflow', 'airflow');

CREATE TABLE taxi_dwh.src_taxi_zone
(
	LocationID UInt16,
	Borough String,
	Zone String,
	service_zone String
)
ENGINE = PostgreSQL('postgres:5432', 'airflow', 'taxi_zone', 'airflow', 'airflow');
```

Создадим таблицу фактов модели "звезда". ORDER BY обязателен, он выполняет функцию организации данных, так как понятия Primary Key в клике нет. Также добавим партиционирование.
```sql
CREATE TABLE taxi_dwh.fact_trips (
-- Keys
taxi_type_id UInt8,
vendor_id UInt8,
ratecode_id UInt8,
PU_location_id UInt16,
DO_location_id UInt16,
payment_id UInt8,

-- Time
pickup_datetime DateTime,
pickup_day UInt8,
pickup_hour UInt8,
dropoff_datetime DateTime,
dropoff_day UInt8,
dropoff_hour UInt8,

-- Metrics
passenger_count UInt8,
trip_distance Float32,
fare_amount Decimal(10,2),
extra Decimal(10,2),
mta_tax Decimal(10,2),
tip_amount Decimal(10,2),
tolls_amount Decimal(10,2),
improvement_surcharge Decimal(10,2),
total_amount Decimal(10,2),
congestion_surcharge Decimal(10,2),
airport_fee Decimal(10,2),
cbd_congestion_fee Decimal(10,2),
)
ENGINE = MergeTree()
PARTITION BY toDate(pickup_datetime)
ORDER BY (pickup_day, pickup_hour, taxi_type_id, vendor_id);
```

Вставим данные в таблицу фактов из таблицы Yellow Tripdata.
```sql
INSERT INTO taxi_dwh.fact_trips
SELECT DISTINCT
	1,
	y.VendorID,
	y.RatecodeID,
	y.PULocationID,
	y.DOLocationID,
	y.payment_type,
	toDateTime(y.tpep_pickup_datetime) as pickup_datetime,
	toDayOfMonth(pickup_datetime),
	toHour(pickup_datetime),
	toDateTime(y.tpep_dropoff_datetime) as dropoff_datetime,
	toDayOfMonth(dropoff_datetime),
	toHour(dropoff_datetime),
	y.passenger_count,
	y.trip_distance,
	y.fare_amount,
	y.extra,
	y.mta_tax,
	y.tip_amount,
	y.tolls_amount,
	y.improvement_surcharge,
	y.total_amount,
	y.congestion_surcharge,
	y.Airport_fee,
	y.cbd_congestion_fee
FROM taxi_dwh.src_ylw_trips y
ORDER BY y.tpep_pickup_datetime;
```

Количество строк за счет SELECT DISTINCT уменьшилось с 11.2 миллиона до 4.2 миллионов.
Проверка количества уникальных строк в исходной таблице и полученной:
```sql
SELECT count(*)
FROM taxi_dwh.fact_trips
```
```
Query id: c6efe868-5386-4052-bfe2-657d714e78bb

┌─count()─┐
│ 4181444 │
└─────────┘

-------------------------------------------------------------------------------
```
```sql
SELECT countDistinct(*)
FROM taxi_dwh.src_ylw_trips
```
```
Query id: 0dbd5f3e-7c88-427f-9bf9-b3db3e33ddf6

┌─uniqExact(VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, Airport_fee, cbd_congestion_fee)─┐
│ 4181444 │
└─────────┘
```

Вставим данные в таблицу фактов из таблицы Green Tripdata.
```sql
INSERT INTO taxi_dwh.fact_trips
SELECT DISTINCT
	2,
	g.VendorID,
	g.RatecodeID,
	g.PULocationID,
	g.DOLocationID,
	g.payment_type,
	toDateTime(g.lpep_pickup_datetime) as pickup_datetime,
	toDayOfMonth(pickup_datetime),
	toHour(pickup_datetime),
	toDateTime(g.lpep_dropoff_datetime) as dropoff_datetime,
	toDayOfMonth(dropoff_datetime),
	toHour(dropoff_datetime),
	g.passenger_count,
	g.trip_distance,
	g.fare_amount,
	g.extra,
	g.mta_tax,
	g.tip_amount,
	g.tolls_amount,
	g.improvement_surcharge,
	g.total_amount,
	g.congestion_surcharge,
	0,
	g.cbd_congestion_fee
FROM taxi_dwh.src_grn_trips g
ORDER BY g.lpep_pickup_datetime;
```

Создадим таблицы измерений.

dim_taxi_type
```sql
CREATE TABLE taxi_dwh.dim_taxi_type (
taxi_type_id UInt8,
name String
) ENGINE = MergeTree()

ORDER BY (taxi_type_id);

INSERT INTO taxi_dwh.dim_taxi_type VALUES
(1, 'Yellow'),
(2, 'Green');
```

dim_vendor
```sql
CREATE TABLE taxi_dwh.dim_vendor (
vendor_id UInt8,
name String
) ENGINE = MergeTree()

ORDER BY (vendor_id);

INSERT INTO taxi_dwh.dim_vendor VALUES
(1, 'Creative Mobile Technologies, LLC'),
(2, 'Curb Mobility, LLC'),
(6, 'Myle Technologies Inc'),
(7, 'Helix');
```

dim_ratecode
```sql
CREATE TABLE taxi_dwh.dim_ratecode (
ratecode_id UInt8,
codename String
) ENGINE = MergeTree()

ORDER BY (ratecode_id);

INSERT INTO taxi_dwh.dim_ratecode VALUES
(1, 'Standard rate'),
(2, 'JFK'),
(3, 'Newark'),
(4, 'Nassau or Westchester'),
(5, 'Negotiated fare'),
(6, 'Group ride'),
(99, 'Null/unknown');
```

dim_location
```sql
CREATE TABLE taxi_dwh.dim_location (
location_id UInt16,
borough String,
zone String,
service_zone String,
) ENGINE = MergeTree()

ORDER BY (location_id);

INSERT INTO taxi_dwh.dim_location
SELECT *
FROM taxi_dwh.src_taxi_zone;
```

dim_payment
```sql
CREATE TABLE taxi_dwh.dim_payment (
payment_id UInt8,
payment_type String
) ENGINE = MergeTree()

ORDER BY (payment_id);

INSERT INTO taxi_dwh.dim_payment VALUES
(0, 'Flex Fare trip'),
(1, 'Credit card'),
(2, 'Cash'),
(3, 'No charge'),
(4, 'Dispute'),
(5, 'Unknown'),
(6, 'Voided trip');
```
