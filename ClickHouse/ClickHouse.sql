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




CREATE TABLE user_profiles
(
    user_id UInt32,                 -- Идентификатор пользователя
    name String,                    -- Имя пользователя
    email String,                   -- Электронная почта
    last_updated DateTime,          -- Время последнего обновления записи
    version UInt32                  -- Версия записи (опционально, для замены на                                              основании версии)
)
ENGINE = ReplacingMergeTree(version)  -- Используем движок ReplacingMergeTree с заменой по полю version
PARTITION BY toYYYYMM(last_updated)   -- Партиционирование по дате последнего                                                    обновления
ORDER BY user_id                      -- Индексация по идентификатору пользователя
PRIMARY KEY user_id                   -- Первичный ключ по user_id
SETTINGS index_granularity = 8192;    -- Размер индекса




INSERT INTO user_profiles (user_id, name, email, last_updated, version)
VALUES 
(1, 'Alice', 'alice@example.com', now(), 1),
(2, 'Bob', 'bob@example.com', now(), 1),
(3, 'Charlie', 'charlie@example.com', now(), 1);

INSERT INTO user_profiles (user_id, name, email, last_updated, version)
VALUES 
(1, 'Alice Cooper', 'alice@example.com', now(), 2);

SELECT * FROM user_profiles FINAL;



