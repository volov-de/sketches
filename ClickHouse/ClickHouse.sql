В каком месяце 2016 года больше перелетов с билетами бизнес класса? 

Июль

SELECT 
    toMonth(scheduled_departure) AS month,
    count(*) AS flight_count
FROM 
    startde.flights AS f
JOIN 
    startde.seats AS s ON s.aircraft_code = f.aircraft_code
WHERE 
   actual_departure - scheduled_departure >
GROUP BY 
    month
ORDER BY 
    flight_count DESC


    
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
    'host:5432', -- хост:порт
    'demo',                                 -- база
    'flights',                              -- таблица в Postgres
    'vj_volov',                             -- пользователь
    'pass', -- пароль
    'bookings'                              -- схема
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
WHERE database = 'startde_student_data' 
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

CREATE TABLE startde_student_data.vj_volov_airports
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

INSERT INTO startde_student_data.vj_volov_airports
SELECT *
FROM postgresql(
    'startde.postgres.karpov.courses:5432',
    'demo',
    'airports',
    'vj_volov',
    '304f084e76a1a85f33eadaf7671813587a9c41535137e8a6ac2bdc43fb858cc7',
    'bookings'
);

--Проверка
SELECT * FROM startde_student_data.vj_volov_airports LIMIT 5;


CREATE TABLE startde_student_data.vj_volov_flights_aggregates
(
    departure_airport String,
    total_flights UInt64,
    delayed_flights UInt64
)
ENGINE = SummingMergeTree
ORDER BY (departure_airport)
SETTINGS index_granularity = 8192;

INSERT INTO startde_student_data.vj_volov_flights_aggregates
SELECT
    f.departure_airport,
    COUNT(*) AS total_flights,
    SUM(CASE 
        WHEN f.actual_departure > f.scheduled_departure THEN 1 
        ELSE 0 
    END) AS delayed_flights
FROM startde_student_data.vj_volov_flights f
GROUP BY f.departure_airport;

select * from startde_student_data.vj_volov_flights_aggregates

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

CREATE TABLE startde_student_data.vj_volov_routes_storage
(
    flight_no String,
    departure_airport String,
    arrival_airport String,
    aircraft_code String,
    duration UInt32
)
ENGINE = ReplacingMergeTree  -- чтобы при дублях оставалась последняя строка
ORDER BY flight_no;

CREATE MATERIALIZED VIEW startde_student_data.vj_volov_routes
TO startde_student_data.vj_volov_routes_storage
AS
SELECT
    flight_no,
    any(departure_airport) AS departure_airport,
    any(arrival_airport) AS arrival_airport,
    any(aircraft_code) AS aircraft_code,
    any(toUInt32(scheduled_arrival - scheduled_departure)) AS duration
FROM startde_student_data.vj_volov_flights
GROUP BY flight_no;

-- Создаём бэкап
CREATE TABLE IF NOT EXISTS startde_student_data.vj_volov_flights_backup
ENGINE = MergeTree ORDER BY flight_id
AS SELECT * FROM startde_student_data.vj_volov_flights;

-- Очищаем
TRUNCATE TABLE startde_student_data.vj_volov_flights;

-- Вставляем обратно — сработает MV
INSERT INTO startde_student_data.vj_volov_flights
SELECT * FROM startde_student_data.vj_volov_flights_backup;

SELECT * FROM startde_student_data.vj_volov_routes_storage limit 5;

--PG0001	UIK	SGC	CR2	8400
--PG0002	SGC	UIK	CR2	8400
--PG0003	IWA	AER	CR2	7800
--PG0004	AER	IWA	CR2	7800
--PG0005	DME	PKV	CN1	7500