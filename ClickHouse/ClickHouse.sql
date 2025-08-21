В каком месяце 2016 года больше перелетов с билетами бизнес класса? 

SELECT 
    toMonth(scheduled_departure) AS month,
    count(*) AS flight_count
FROM 
    flights AS f
JOIN 
    seats AS s ON s.aircraft_code = f.aircraft_code
WHERE 
   actual_departure - scheduled_departure >
GROUP BY 
    month
ORDER BY 
    flight_count DESC
Ответ: Июль

    
У скольких пассажиров рейс был задержан более чем на 3 часа?
    
SELECT
   COUNT(DISTINCT t.passenger_id) AS delayed_passengers
FROM
   flights f
JOIN
   ticket_flights tf ON f.flight_id = tf.flight_id
JOIN
   tickets t ON tf.ticket_no = t.ticket_no
WHERE 
     dateDiff('second', scheduled_departure, actual_departure) > 10800;



Нам поручили загрузить данные о перелетах из PG и сделать по ним агрегационную таблицу для аналитиков.
Вам необходимо настроить интеграцию с PG. Создайте репликационный движок к таблице flights, назовите ее flights_remote

DROP TABLE vj_volov_flights_remote;

CREATE TABLE vj_volov_flights_remote
(
    `flight_id` UInt32,
    `flight_no` String,
    `scheduled_departure` DateTime,
    `scheduled_arrival` DateTime,
    `departure_airport` String,
    `arrival_airport` String,
    `status` String,
    `aircraft_code` String,
    `actual_departure` Nullable(DateTime),
    `actual_arrival` Nullable(DateTime)
)
ENGINE = PostgreSQL(
    'host:5432', 
    'demo',
    'flights',
    'vj_volov',
    'pass',
    'bookings'
);


Создайте таблицу flights c типом mergetree и партицией по месяцам запланированной даты вылета
Заполните ее из flights_remote

CREATE TABLE vj_volov_flights
(
    `flight_id` UInt32,
    `flight_no` String,
    `scheduled_departure` DateTime,
    `scheduled_arrival` DateTime,
    `departure_airport` String,
    `arrival_airport` String,
    `status` String,
    `aircraft_code` String,
    `actual_departure` Nullable(DateTime),
    `actual_arrival` Nullable(DateTime)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(scheduled_departure)
ORDER BY (flight_id)
SETTINGS index_granularity = 8192;

--Загрузка данные
INSERT INTO vj_volov_flights
SELECT *
FROM vj_volov_flights_remote;

--Проверка: сколько строк загружено
SELECT count(*) AS row_count
FROM vj_volov_flights;
--214867

--Проверка партиций
SELECT 
    partition,
    sum(rows) AS rows_in_partition
FROM system.parts 
WHERE database = 'student_data' 
  AND table = 'vj_volov_flights'
GROUP BY partition
ORDER BY partition;

--partition	rows_in_partition
--201510	9780
--201511	16254
--201512	16831
--201601	16783
--201602	15760
--201603	16831
--201604	16289
--201605	16811
--201606	16274
--201607	16783
--201608	16853
--201609	16286
--201610	16803
--201611	6529


Создайте агрегационную суммирующую таблицу, которая считает
по каждому городу вылета количество вылетов (total_flights)
и количество вылетов с опозданием (delayed_flights). 
Назовите ее flights_aggregates

CREATE TABLE student_data.vj_volov_airports
(
    airport_code String,
    airport_name String,
    city String,
    longitude Float64,
    latitude Float64,
    timezone String
)
ENGINE = MergeTree
ORDER BY airport_code;

INSERT INTO student_data.vj_volov_airports
SELECT *
FROM postgresql(
    'postgres.karpov.courses:5432',
    'demo',
    'airports',
    'vj_volov',
    '304f084e76a1a85f33eadaf7671813587a9c41535137e8a6ac2bdc43fb858cc7',
    'bookings'
);

--Проверка
SELECT * FROM student_data.vj_volov_airports LIMIT 5;


CREATE TABLE student_data.vj_volov_flights_aggregates
(
    departure_airport String,
    total_flights UInt64,
    delayed_flights UInt64
)
ENGINE = SummingMergeTree
ORDER BY (departure_airport)
SETTINGS index_granularity = 8192;

INSERT INTO student_data.vj_volov_flights_aggregates
SELECT
    f.departure_airport,
    COUNT(*) AS total_flights,
    SUM(CASE 
        WHEN f.actual_departure > f.scheduled_departure THEN 1 
        ELSE 0 
    END) AS delayed_flights
FROM student_data.vj_volov_flights f
GROUP BY f.departure_airport;

select * from student_data.vj_volov_flights_aggregates

--departure_airport, total_flights, delayed_flights
--AAQ	849	751
--ASF	961	855
--NFG	283	251
--NUX	3282	2897
--SKX	792	695




Чтобы эффективно планировать рейсы и улучшать клиентский сервис, вашей команде аналитиков требуется 
централизованное место хранения всей информация о маршрутах.
Создайте материализованное представление routes со следующими полями с запросом на базе таблицы flights:
flight_no (первичный ключ) — номер рейса
departure_airport — код аэропорта вылета
arrival_airport — код аэропорта прилета
aircraft_code — код самолета
duration — продолжительность полета, которая будет рассчитана как разница между запланированными временами вылета и прилета

CREATE TABLE student_data.vj_volov_routes_storage
(
    flight_no String,
    departure_airport String,
    arrival_airport String,
    aircraft_code String,
    duration UInt32
)
ENGINE = ReplacingMergeTree  -- чтобы при дублях оставалась последняя строка
ORDER BY flight_no;

CREATE MATERIALIZED VIEW student_data.vj_volov_routes
TO student_data.vj_volov_routes_storage
AS
SELECT
    flight_no,
    any(departure_airport) AS departure_airport,
    any(arrival_airport) AS arrival_airport,
    any(aircraft_code) AS aircraft_code,
    any(toUInt32(scheduled_arrival - scheduled_departure)) AS duration
FROM student_data.vj_volov_flights
GROUP BY flight_no;

-- Создаём бэкап
CREATE TABLE IF NOT EXISTS student_data.vj_volov_flights_backup
ENGINE = MergeTree ORDER BY flight_id
AS SELECT * FROM student_data.vj_volov_flights;

-- Очищаем
TRUNCATE TABLE student_data.vj_volov_flights;

-- Вставляем обратно — сработает MV
INSERT INTO student_data.vj_volov_flights
SELECT * FROM student_data.vj_volov_flights_backup;

SELECT * FROM student_data.vj_volov_routes_storage limit 5;

--PG0001	UIK	SGC	CR2	8400
--PG0002	SGC	UIK	CR2	8400
--PG0003	IWA	AER	CR2	7800
--PG0004	AER	IWA	CR2	7800
--PG0005	DME	PKV	CN1	7500

Ваша команда получает данные о новых бронированиях для системы управления авиабилетами в формате JSON. 
Ваша задача заключается в создании процесса загрузки информации о бронированиях и билетах из предоставленного JSON в таблицы вашей базы данных.

Информация приходит в следующем виде (таблица bookings_json)

book_ref bpchar(6)
book_date timestamptz
total_amount numeric(10, 2)
json_data json

В поле json_data находится json следующего вида

список покупателей билетов
для каждого билета список перелетов


-- Таблица bookings_raw
CREATE TABLE vj_volov_bookings_raw (
    book_ref String,
    book_date DateTime64(0, 'UTC'),
    total_amount String
) ENGINE = MergeTree()
ORDER BY book_ref;

-- Таблица tickets_raw
CREATE TABLE vj_volov_tickets_raw (
    ticket_no String,
    book_ref String,
    passenger_id String,
    passenger_name String,
    contact_data String
) ENGINE = MergeTree()
ORDER BY (book_ref, ticket_no);

-- Таблица ticket_flights_raw
CREATE TABLE vj_volov_ticket_flights_raw (
    ticket_no String,
    flight_id String,
    fare_conditions String,
    amount String
) ENGINE = MergeTree()
ORDER BY (ticket_no, flight_id);

--Загрузка данных с дедубликацией
--Загрузка в bookings_raw

INSERT INTO vj_volov_bookings_raw (book_ref, book_date, total_amount)
SELECT DISTINCT
    book_ref,
    parseDateTime64BestEffort(book_date) as book_date,
    toString(total_amount)
FROM bookings_json;


--Загрузка в tickets_raw
INSERT INTO vj_volov_tickets_raw (ticket_no, book_ref, passenger_id, passenger_name, contact_data)
SELECT DISTINCT
    JSONExtractString(ticket_data, 'ticket_no') as ticket_no,
    book_ref,
    JSONExtractString(ticket_data, 'passenger_id') as passenger_id,
    JSONExtractString(ticket_data, 'passenger_name') as passenger_name,
    JSONExtractString(ticket_data, 'contact_data') as contact_data
FROM (
    SELECT 
        book_ref,
        ticket_data
    FROM bookings_json
    ARRAY JOIN JSONExtractArrayRaw(json_data) as ticket_data
);

--Загрузка в ticket_flights_raw
INSERT INTO vj_volov_ticket_flights_raw (ticket_no, flight_id, fare_conditions, amount)
SELECT DISTINCT
    ticket_no,
    JSONExtractString(flight_data, 'flight_id') as flight_id,
    JSONExtractString(flight_data, 'fare_conditions') as fare_conditions,
    JSONExtractString(flight_data, 'amount') as amount
FROM (
    SELECT 
        JSONExtractString(ticket_data, 'ticket_no') as ticket_no,
        JSONExtractString(ticket_data, 'flights') as flights_json
    FROM bookings_json
    ARRAY JOIN JSONExtractArrayRaw(json_data) as ticket_data
) AS tickets_with_flights
ARRAY JOIN JSONExtractArrayRaw(flights_json) as flight_data;

-- Проверка bookings_raw
SELECT * FROM vj_volov_bookings_raw LIMIT 10;

-- Проверка tickets_raw
SELECT * FROM vj_volov_tickets_raw LIMIT 10;

-- Проверка ticket_flights_raw
SELECT * FROM vj_volov_ticket_flights_raw LIMIT 10;
	



Создайте одну большую таблицу, в которой будут поля из tickets_raw, ticket_flights_raw, flights, bookings_raw. 
Назовите ее fct_flights_mart

CREATE TABLE vj_volov_fct_flights_mart (
    ticket_no String,
    book_ref String,
    passenger_id String,
    passenger_name String,
    flight_id UInt32,
    fare_conditions String,
    amount String,
    flight_no String,
    scheduled_departure DateTime,
    scheduled_arrival DateTime,
    departure_airport String,
    arrival_airport String,
    status String,
    aircraft_code String,
    actual_departure DateTime,
    actual_arrival DateTime,
    book_date String,
    total_amount String,
    contact_data String
) ENGINE = MergeTree()
ORDER BY (book_ref, ticket_no, flight_id);

INSERT INTO vj_volov_fct_flights_mart (
    ticket_no,
    book_ref,
    passenger_id,
    passenger_name,
    flight_id,
    fare_conditions,
    amount,
    flight_no,
    scheduled_departure,
    scheduled_arrival,
    departure_airport,
    arrival_airport,
    status,
    aircraft_code,
    actual_departure,
    actual_arrival,
    book_date,
    total_amount,
    contact_data
)
SELECT 
    t.ticket_no,
    t.book_ref,
    t.passenger_id,
    t.passenger_name,
    toUInt32OrNull(tf.flight_id) as flight_id,
    tf.fare_conditions,
    tf.amount,
    f.flight_no,
    f.scheduled_departure,
    f.scheduled_arrival,
    f.departure_airport,
    f.arrival_airport,
    f.status,
    f.aircraft_code,
    coalesce(f.actual_departure, toDateTime('1970-01-01 00:00:00')) as actual_departure,
    coalesce(f.actual_arrival, toDateTime('1970-01-01 00:00:00')) as actual_arrival,
    toString(b.book_date) as book_date,
    b.total_amount,
    t.contact_data
FROM vj_volov_bookings_raw b
JOIN vj_volov_tickets_raw t ON b.book_ref = t.book_ref
JOIN vj_volov_ticket_flights_raw tf ON t.ticket_no = tf.ticket_no
LEFT JOIN vj_volov_flights f ON toUInt32OrNull(tf.flight_id) = f.flight_id;


SELECT count(*) as total_records FROM vj_volov_fct_flights_mart;




Специфичные джойны ASOF JOIN
Создайте витрину fct_flights_weather_mart
Используйте таблицу, полученную в предыдущем задании и таблицу weather_data_hourly
Состав полей таблицы weather_data_hourly

timestamp-- Временная метка с часовым шагом
airport-- Код аэропорта
temperature-- температура от -10 до +30 градусов Цельсия
humidity-- влажность от 0% до 100%
wind_speed-- скорость ветра от 0 до 20 м/с
condition-- погодное условие
Используйте ASOF join для поиска ближайшего погодного условия ко времени актуального вылета (departure_) и прилета (arrival_)

Добавьте в витрину fct_flights_weather_mart столбцы
temperature(Float32) 
humidity (UInt8) 
wind_speed (Float32)
condition(String)
для погоды в аэропорте вылета (departure_) и аэропорте прилета (arrival_).

В самой витрине отфильтруйте только status = 'Arrived'

CREATE TABLE vj_volov_fct_flights_weather_mart (
    flight_id UInt32,
    flight_no String,
    scheduled_departure DateTime,
    scheduled_arrival DateTime,
    departure_airport String,
    arrival_airport String,
    status String,
    aircraft_code String,
    actual_departure DateTime,
    actual_arrival DateTime,
    fare_conditions String,
    amount String,
    ticket_no String,
    book_ref String,
    passenger_id String,
    passenger_name String,
    contact_data String,
    book_date String,
    total_amount String,
    departure_temperature Float32,
    departure_humidity UInt8,
    departure_wind_speed Float32,
    departure_condition String,
    arrival_temperature Float32,
    arrival_humidity UInt8,
    arrival_wind_speed Float32,
    arrival_condition String
) ENGINE = MergeTree()
ORDER BY (book_ref, ticket_no, flight_id);

INSERT INTO vj_volov_fct_flights_weather_mart (
    flight_id,
    flight_no,
    scheduled_departure,
    scheduled_arrival,
    departure_airport,
    arrival_airport,
    status,
    aircraft_code,
    actual_departure,
    actual_arrival,
    fare_conditions,
    amount,
    ticket_no,
    book_ref,
    passenger_id,
    passenger_name,
    contact_data,
    book_date,
    total_amount,
    departure_temperature,
    departure_humidity,
    departure_wind_speed,
    departure_condition,
    arrival_temperature,
    arrival_humidity,
    arrival_wind_speed,
    arrival_condition
)
SELECT 
    fm.flight_id,
    fm.flight_no,
    fm.scheduled_departure,
    fm.scheduled_arrival,
    fm.departure_airport,
    fm.arrival_airport,
    fm.status,
    fm.aircraft_code,
    coalesce(fm.actual_departure, toDateTime('1970-01-01 00:00:00')) as actual_departure,
    coalesce(fm.actual_arrival, toDateTime('1970-01-01 00:00:00')) as actual_arrival,
    fm.fare_conditions,
    fm.amount,
    fm.ticket_no,
    fm.book_ref,
    fm.passenger_id,
    fm.passenger_name,
    fm.contact_data,
    toString(fm.book_date) as book_date,
    fm.total_amount,
    w_dep.temperature as departure_temperature,
    w_dep.humidity as departure_humidity,
    w_dep.wind_speed as departure_wind_speed,
    w_dep.condition as departure_condition,
    w_arr.temperature as arrival_temperature,
    w_arr.humidity as arrival_humidity,
    w_arr.wind_speed as arrival_wind_speed,
    w_arr.condition as arrival_condition
FROM vj_volov_fct_flights_mart fm
LEFT ASOF JOIN weather_data_hourly w_dep
    ON (fm.departure_airport = w_dep.airport AND fm.actual_departure >= w_dep.timestamp)
LEFT ASOF JOIN weather_data_hourly w_arr
    ON (fm.arrival_airport = w_arr.airport AND fm.actual_arrival >= w_arr.timestamp)
WHERE fm.status = 'Arrived'
  AND fm.actual_departure IS NOT NULL 
  AND fm.actual_arrival IS NOT NULL
  AND fm.actual_departure > toDateTime('1970-01-01 00:00:00')
  AND fm.actual_arrival > toDateTime('1970-01-01 00:00:00');







Нам надо оптимизировать таблицу fct_flights_weather_mart
Мы это будем делать пошагово
Оптимизация на базе запросов
>>  Шаг 1
Большая часть запросов идет с использованием даты scheduled_departure
Давайте добавим партицию по месяцу в таблицу
>>  Шаг 2
Часть запросов используют единичные flight_id
Для ускорения таких запросов добавим первичный ключ и сортировку по flight_id
>>  Шаг  3
Аналитики часто фильтруют по аэропорту прилета и вылета
Добавим скип индекс по этим полям

CREATE TABLE vj_volov_fct_flights_weather_mart_opt (
    flight_id UInt32,
    flight_no String,
    scheduled_departure DateTime,
    scheduled_arrival DateTime,
    departure_airport String,
    arrival_airport String,
    status String,
    aircraft_code String,
    actual_departure DateTime,
    actual_arrival DateTime,
    fare_conditions String,
    amount String,
    ticket_no String,
    book_ref String,
    passenger_id String,
    passenger_name String,
    contact_data String,
    book_date String,
    total_amount String,
    departure_temperature Float32,
    departure_humidity UInt8,
    departure_wind_speed Float32,
    departure_condition String,
    arrival_temperature Float32,
    arrival_humidity UInt8,
    arrival_wind_speed Float32,
    arrival_condition String,
    INDEX airports_idx (departure_airport, arrival_airport) TYPE minmax GRANULARITY 1
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(scheduled_departure)
PRIMARY KEY flight_id
ORDER BY flight_id
SETTINGS index_granularity = 8192;

INSERT INTO vj_volov_fct_flights_weather_mart_opt
SELECT 
    flight_id,
    flight_no,
    scheduled_departure,
    scheduled_arrival,
    departure_airport,
    arrival_airport,
    status,
    aircraft_code,
    actual_departure,
    actual_arrival,
    fare_conditions,
    amount,
    ticket_no,
    book_ref,
    passenger_id,
    passenger_name,
    contact_data,
    book_date,
    total_amount,
    departure_temperature,
    departure_humidity,
    departure_wind_speed,
    departure_condition,
    arrival_temperature,
    arrival_humidity,
    arrival_wind_speed,
    arrival_condition
FROM vj_volov_fct_flights_weather_mart;


-- Проверка структуры таблицы
DESCRIBE TABLE vj_volov_fct_flights_weather_mart_opt;


-- Проверка количества записей
SELECT count(*) as total_records FROM vj_volov_fct_flights_weather_mart_opt;

-- Проверка партиций
SELECT 
    partition,
    count(*) as rows_count
FROM system.parts 
WHERE table = 'vj_volov_fct_flights_weather_mart_opt' 
  AND active = 1
GROUP BY partition
ORDER BY partition;


Оптимизация таблицы. Оптимизация хранения
Оптимизация хранения
>> Шаг 1
Для уменьшения объема таблицы приведем типы полей в порядок
Проверьте все текстовые поля и приведите данные к оптимальным типам
UInt8 \ Int8 - для небольших целочисленных
UInt32 - для больших целочисленных
Datetime - для даты
>> Шаг 2
Для уменьшения объема таблицы приведем типы полей в порядок
В нашей таблице есть текстовые поля фиксированной длины, для них можно использовать FixedString
Замените все такие поля
>> Шаг 3
json в строке хранить менее эффективно, давайте удалим contact_data и заменим их на
passenger_phone и passenger_email
>> Шаг 4
Для времени и температур можно добавить CODEC(Delta, ZSTD)
Чтобы изменение было более плавное, добавим в сортировку таблицы поля 
scheduled_departure, 
departure_airport, 
arrival_airport
>> Шаг 5
Все текстовые поля с низкой кардинальностью можно заменить на LowCardinality (аэропорт прилета\вылета, 
код самолета) или на Enum8(статус полета, погодные условия в аэропорту)
В результате преобразований у нас должна получиться таблица определённого размера (менее 555Mb) 
с нужными партициями, сортировкой и индексами.

DROP TABLE IF EXISTS vj_volov_fct_flights_weather_mart_opt;

CREATE TABLE vj_volov_fct_flights_weather_mart_opt
(
    `ticket_no` FixedString(13),
    `book_ref` FixedString(6),
    `passenger_id` String,
    `passenger_name` String,
    `passenger_email` String,
    `passenger_phone` String,
    `flight_id` UInt32,
    `fare_conditions` LowCardinality(String),
    `amount` Float32,
    `flight_no` FixedString(6),
    `scheduled_departure` DateTime CODEC(Delta(4), ZSTD(1)),
    `scheduled_arrival` DateTime CODEC(Delta(4), ZSTD(1)),
    `departure_airport` LowCardinality(FixedString(3)),
    `arrival_airport` LowCardinality(FixedString(3)),
    `status` Enum8('Scheduled' = 1, 'On Time' = 2, 'Delayed' = 3, 'Departed' = 4, 'Arrived' = 5, 'Cancelled' = 6),
    `aircraft_code` LowCardinality(FixedString(3)),
    `actual_departure` Nullable(DateTime) CODEC(Delta(4), ZSTD(1)),
    `actual_arrival` Nullable(DateTime) CODEC(Delta(4), ZSTD(1)),
    `book_date` DateTime CODEC(Delta(4), ZSTD(1)),
    `total_amount` Float32,
    `departure_temperature` Float32 CODEC(Delta(4), ZSTD(1)),
    `departure_humidity` UInt8,
    `departure_wind_speed` Float32,
    `departure_condition` Enum8('Clear' = 1, 'Rain' = 2, 'Cloudy' = 3, 'Snow' = 4, 'Thunderstorm' = 5),
    `arrival_temperature` Float32 CODEC(Delta(4), ZSTD(1)),
    `arrival_humidity` UInt8,
    `arrival_wind_speed` Float32,
    `arrival_condition` Enum8('Clear' = 1, 'Rain' = 2, 'Cloudy' = 3, 'Snow' = 4, 'Thunderstorm' = 5),
    INDEX airports_idx (departure_airport, arrival_airport) TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(scheduled_departure)
PRIMARY KEY flight_id
ORDER BY (flight_id, scheduled_departure, departure_airport, arrival_airport)
SETTINGS index_granularity = 8192;

INSERT INTO vj_volov_fct_flights_weather_mart_opt (
    ticket_no,
    book_ref,
    passenger_id,
    passenger_name,
    passenger_email,
    passenger_phone,
    flight_id,
    fare_conditions,
    amount,
    flight_no,
    scheduled_departure,
    scheduled_arrival,
    departure_airport,
    arrival_airport,
    status,
    aircraft_code,
    actual_departure,
    actual_arrival,
    book_date,
    total_amount,
    departure_temperature,
    departure_humidity,
    departure_wind_speed,
    departure_condition,
    arrival_temperature,
    arrival_humidity,
    arrival_wind_speed,
    arrival_condition
)
SELECT 
    ticket_no,
    book_ref,
    passenger_id,
    passenger_name,
    JSONExtractString(contact_data, 'email') as passenger_email,
    JSONExtractString(contact_data, 'phone') as passenger_phone,
    flight_id,
    fare_conditions,
    toFloat32(amount),
    flight_no,
    scheduled_departure,
    scheduled_arrival,
    departure_airport,
    arrival_airport,
    status,
    aircraft_code,
    actual_departure,
    actual_arrival,
    parseDateTime64BestEffort(book_date) as book_date,
    toFloat32(total_amount),
    departure_temperature,
    departure_humidity,
    departure_wind_speed,
    departure_condition,
    arrival_temperature,
    arrival_humidity,
    arrival_wind_speed,
    arrival_condition
FROM vj_volov_fct_flights_weather_mart;

-- Проверка структуры таблицы
DESCRIBE TABLE vj_volov_fct_flights_weather_mart_opt;

-- Проверка количества записей
SELECT count(*) as total_records FROM vj_volov_fct_flights_weather_mart_opt;

-- Проверка размера таблицы
SELECT 
    table,
    formatReadableSize(sum(bytes)) as size,
    sum(rows) as rows
FROM system.parts 
WHERE table = 'vj_volov_fct_flights_weather_mart_opt' 
  AND active = 1
GROUP BY table;

-- Проверка примера данных
SELECT 
    ticket_no,
    book_ref,
    passenger_name,
    passenger_email,
    passenger_phone,
    flight_id,
    fare_conditions,
    amount,
    scheduled_departure,
    departure_airport,
    arrival_airport,
    status
FROM vj_volov_fct_flights_weather_mart_opt 
LIMIT 5;