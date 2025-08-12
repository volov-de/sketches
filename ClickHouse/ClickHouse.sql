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

DROP TABLE startde_student_data.vj_volov_flights_remote;

CREATE TABLE startde_student_data.vj_volov_flights_remote
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

CREATE TABLE startde_student_data.vj_volov_flights
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
INSERT INTO startde_student_data.vj_volov_flights
SELECT *
FROM startde_student_data.vj_volov_flights_remote;

--Проверка: сколько строк загружено
SELECT count(*) AS row_count
FROM startde_student_data.vj_volov_flights;
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



