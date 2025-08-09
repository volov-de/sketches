# SQL Скрипт для создания таблиц и представлений в PostgreSQL

## Описание проекта

Этот скрипт предназначен для создания структуры базы данных с учетом требований аналитики и мониторинга полетов. Включает создание таблиц, представлений, функций и процедур для эффективной работы с данными о рейсах, пассажирах и бронированиях.

Таблица bookings.seats
Места определяют схему салона каждой модели самолета (aircraft_code). 
Каждое место имеет свой номер (seat_no) и закрепленный за ним класс обслуживания (fare_conditions) — Economy, Comfort или Business. 
Количество мест (Seats) в самолете и их распределение по классам обслуживания зависит от модели самолета (Aircrafts), выполняющего рейс. 
Предполагается, что каждая модель самолета имеет только одну компоновку салона.

Таблица bookings.aircrafts 
Каждая модель воздушного судна идентифицируется своим трехзначным кодом (aircraft_code). 
Указывается также название модели (model) и максимальная дальность полета в километрах (range).

Таблица bookings.flights
Естественный ключ таблицы рейсов состоит из двух полей — номера рейса (flight_no) и даты отправления (scheduled_departure). 
Чтобы сделать внешние ключи на эту таблицу компактнее, в качестве первичного используется суррогатный ключ (flight_id). 
Рейс всегда соединяет две точки — аэропорты вылета (departure_airport) и прибытия (arrival_airport). 
Такое понятие, как «рейс с пересадками» отсутствует: если из одного аэропорта до другого нет прямого рейса, в билет просто включаются несколько необходимых рейсов. 
У каждого рейса есть запланированные дата и время вылета (scheduled_departure) и прибытия (scheduled_arrival). 
Реальные время вылета (actual_departure) и прибытия (actual_arrival) могут отличаться: обычно не сильно, но иногда и на несколько часов, если рейс задержан.

Таблица bookings.airports
Аэропорт идентифицируется трехбуквенным кодом (airport_code) и имеет свое имя (airport_name). 
Для города не предусмотрено отдельной сущности, но название (city) указывается и может служить для того, чтобы определить аэропорты одного города. 
Также указывается широта (longitude), долгота (latitude) и часовой пояс (timezone).



Создание таблицы
Как было сказано в описании схемы: для города не предусмотрено отдельной сущности. Но мы считаем, что она будет полезна для нас и наших коллег-аналитиков, поэтому выделим её.
Создайте и заполните данными таблицу cities со следующими полями:

city_id (тип serial, не может быть NULL) — уникальный идентификатор города
city_name (тип text, не может быть NULL) — название города
latitude (тип double precision, не может быть NULL) — широта города
longitude (тип double precision, не может быть NULL) — долгота города
timezone (тип text, может быть NULL) — временная зона, в которой находится город
 Добавьте ограничения на поля широты и долготы (если вам нужно задать название для ограничений, используйте cities_latitude_check и cities_longitude_check).
 Убедитесь, что city_id является первичным ключом таблицы.

CREATE TABLE cities (
    city_id      SERIAL PRIMARY KEY NOT NULL,
    city_name    text NOT NULL,
    latitude     double precision NOT NULL,
    longitude    double precision NOT NULL,
    timezone     text,
    CONSTRAINT cities_latitude_check  CHECK (latitude BETWEEN -90 AND 90),
    CONSTRAINT cities_longitude_check CHECK (longitude BETWEEN -180 AND 180)
);

-- Заполнение данными из airports
INSERT INTO cities (city_name, latitude, longitude, timezone)
SELECT
    a.city as city_name,
    a.latitude,
    a.longitude,
    timezone
FROM bookings.airports a;





Изменение таблицы
Модифицируйте созданную таблицу cities с помощью команды ALTER.
1. Добавьте два новых поля в таблицу cities:
airport_count (тип integer, может быть NULL) — количество аэропортов в данном городе
departure_count (тип integer, может быть NULL) — количество вылетов за все время
2. Измените тип данных для поля city_name:
Установите тип данных varchar(255) вместо text. Это позволит ограничить длину названия города и лучше отразит специфику хранения данных
3. Добавьте ограничение на поле airport_count ( назовите ограничение airport_count_nonnegative):
Установите ограничение, чтобы количество аэропортов было неотрицательным

ALTER TABLE cities
ADD COLUMN airport_count integer;
-- Добавление количества вылетов за все время
ALTER TABLE cities
ADD COLUMN departure_count integer;
-- Изменение типа данных с text на varchar(255)
ALTER TABLE cities
ALTER COLUMN city_name
TYPE varchar(255);
-- Добавление ограничения на неотрицательность количества аэропортов
ALTER TABLE cities
ADD CONSTRAINT airport_count_nonnegative CHECK (airport_count >= 0);





Создание view
Вам поручено создать представление, которое объединяет информацию об аэропортах вылета и прилета для всех рейсов. 
Это представление будет служить важным инструментом для анализа и мониторинга полетов, облегчающим доступ к ключевой информации о каждом рейсе.

Создайте и заполните данными view с названием flight_airport_info, в котором будет содержаться:
номер рейса, идентификатор рейса и запланированное время вылета и прибытия,
вся информация об аэропортах прилета (используйте префикс для имен полей arrival_)
вся информация об аэропортах вылета (используйте префикс для имен полей departure_)
код воздушного судна, актуальное время вылета и прибытия, статус рейса
Например, правильное использование префикса для города вылета – departure_city

-- Создание представления с информацией о рейсах и аэропортах
CREATE OR REPLACE VIEW flight_airport_info AS
SELECT
    f.flight_no,
    f.flight_id,
    f.scheduled_departure,
    f.scheduled_arrival,
    f.aircraft_code,
    f.actual_departure,
    f.actual_arrival,
    f.status,
    dep.airport_code   AS departure_airport,
    dep.airport_name   AS departure_airport_name,
    dep.city           AS departure_city,
    dep.longitude      AS departure_longitude,
    dep.latitude       AS departure_latitude,
    dep.timezone       AS departure_timezone,
    arr.airport_code   AS arrival_airport,
    arr.airport_name   AS arrival_airport_name,
    arr.city           AS arrival_city,
    arr.longitude      AS arrival_longitude,
    arr.latitude       AS arrival_latitude,
    arr.timezone       AS arrival_timezone
FROM bookings.flights f
JOIN bookings.airports dep ON f.departure_airport = dep.airport_code
JOIN bookings.airports arr ON f.arrival_airport = arr.airport_code;





Создание таблицы маршрутов
Чтобы эффективно планировать рейсы и улучшать клиентский сервис, вашей команде аналитиков требуется централизованное место хранения всей информация о маршрутах.
Создайте таблицу routes со следующими полями:

flight_no (тип char(6), первичный ключ) — номер рейса
departure_airport (тип char(3)) — код аэропорта вылета
arrival_airport (тип char(3)) — код аэропорта прилета
aircraft_code (тип char(3)) — код самолета
duration (тип interval) — продолжительность полета, которая будет рассчитана как разница между запланированными временами вылета и прилета
Заполните таблицу данными.

-- Создание таблицы маршрутов
CREATE TABLE routes (
    flight_no CHAR(6) PRIMARY KEY,
    departure_airport CHAR(3),
    arrival_airport CHAR(3),
    aircraft_code CHAR(3),
    duration INTERVAL
);
-- Заполнение данными из flights
INSERT INTO routes (flight_no, departure_airport, arrival_airport, aircraft_code, duration)
SELECT
    flight_no,
    departure_airport,
    arrival_airport,
    aircraft_code,
    MIN(scheduled_arrival) - MIN(scheduled_departure) AS duration
FROM bookings.flights
GROUP BY flight_no, departure_airport, arrival_airport, aircraft_code;





Заполнение данными из JSON
Ваша команда получает данные о новых бронированиях для системы управления авиабилетами в формате JSON. 
Ваша задача заключается в создании процесса загрузки информации о бронированиях и билетах из предоставленного JSON в таблицы вашей базы данных.
Информация приходит в следующем виде (таблица bookings_json):

book_ref bpchar(6)
book_date timestamptz
total_amount numeric(10, 2)
json_data json
В поле json_data находится json следующего вида:

информация о покупателе билета
список перелетов для каждого билета

На базе таблицы bookings_json необходимо создать и заполнить таблицы
Вам нужно создать три таблицы – будем создавать по одной на следующих трех степах.

Заполнение данными из JSON. Шаг 1
Напишите запрос для создания таблицы Bookings на базе таблицы bookings_json и заполните её.
Поля таблицы:

book_ref bpchar(6) NOT NULL
book_date timestamptz NOT NULL
total_amount numeric(10, 2) NOT NULL

-- Создание таблицы бронирований
CREATE TABLE bookings (
    book_ref bpchar(6) PRIMARY KEY,
    book_date timestamptz NOT NULL,
    total_amount numeric(10,2) NOT NULL
);

-- Заполнение данными из JSON
INSERT INTO bookings (book_ref, book_date, total_amount)
SELECT
    book_ref,
    book_date,
    total_amount
FROM bookings.bookings_json;





Заполнение данными из JSON. Шаг 2
Напишите запрос для чтения таблицы bookings_json и создания таблицы tickets_raw c текстовыми полями.
Затем создайте view c приведением типов к правильным:

ticket_no bpchar(13)
book_ref bpchar(6)
passenger_id varchar(20)
passenger_name text
contact_data jsonb

-- Создание временной таблицы для билетов
DROP TABLE IF EXISTS tickets_raw CASCADE ;
CREATE TABLE tickets_raw AS
SELECT
    (js_elem->>'ticket_no') AS ticket_no,
    bj.book_ref AS book_ref,
    (js_elem->>'passenger_id') AS passenger_id,
    (js_elem->>'passenger_name') AS passenger_name,
    js_elem->>'contact_data' AS contact_data
FROM bookings.bookings_json bj,
     LATERAL jsonb_array_elements(bj.json_data::jsonb) AS js_elem;






Заполнение данными из JSON. Шаг 3
Напишите запрос для чтения таблицы bookings_json и создания таблицы ticket_flights_raw c текстовыми полями.
Затем создайте view c приведением типов к правильным:

ticket_no bpchar(13)
flight_id int4
fare_conditions varchar(10)
amount numeric(10, 2)

-- Создание временной таблицы для билетов и рейсов
CREATE TABLE ticket_flights_raw AS
SELECT
    (js_elem->>'ticket_no')       AS ticket_no,
    (flight->>'flight_id')        AS flight_id,
    (flight->>'fare_conditions')  AS fare_conditions,
    (flight->>'amount')           AS amount
FROM bookings.bookings_json bj,
     LATERAL jsonb_array_elements(bj.json_data::jsonb) AS js_elem,
     LATERAL jsonb_array_elements(js_elem->'flights') AS flight;



-- Создание представления ticket_flights
CREATE OR REPLACE VIEW vj_volov.ticket_flights AS
SELECT
    ticket_no::bpchar(13)           AS ticket_no,
    flight_id::int4                 AS flight_id,
    fare_conditions::varchar(10)    AS fare_conditions,
    amount::numeric(10,2)           AS amount
FROM vj_volov.ticket_flights_raw;

-- Создание представления tickets
CREATE OR REPLACE VIEW tickets AS
SELECT
    ticket_no::bpchar(13)     AS ticket_no,
    book_ref::bpchar(6)       AS book_ref,
    passenger_id::varchar(20) AS passenger_id,
    passenger_name::text      AS passenger_name,
    contact_data::jsonb       AS contact_data
FROM tickets_raw;






Функции для номера места
Ваша задача заключается в написании функций для обработки информации о местах в самолете и создании таблицы для хранения данных о посадочных талонах.

1. Напишите две функции:
get_row — должна принимать на вход seat (номер места в формате, например, "12A") и возвращать соответствующий ряд (например, 12)
get_seat — должна принимать на вход seat и возвращать номер кресла (например, из "12А" она вернет "А")
2. Создайте таблицу boarding_passes
Таблица должна содержать все поля из оригинальной таблицы boarding_passes, а также два дополнительных поля:
row (тип integer или varchar, в зависимости от вашей реализации функции get_row для хранения ряда)
seat (тип char(1) или varchar(1) для хранения номера кресла, полученного с помощью функции get_seat)

-- Создание функций для извлечения данных из номера места
CREATE OR REPLACE FUNCTION get_row(seat_no text) 
RETURNS integer AS $$
    SELECT regexp_replace(seat_no, '[^0-9]', '', 'g')::integer;
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION get_seat(seat_no text) 
RETURNS char(1) AS $$
    SELECT regexp_replace(seat_no, '[^A-Z]', '', 'g')::char(1);
$$ LANGUAGE sql;

-- Создание таблицы посадочных талонов
DROP TABLE IF EXISTS boarding_passes CASCADE ;
CREATE TABLE boarding_passes (
    ticket_no bpchar(13) NOT NULL,
    flight_id int4 NOT NULL,
    boarding_no int4 NOT NULL,
    seat_no varchar(4) NOT NULL,
    row integer NOT NULL,
    seat char(1) NOT NULL
);

-- Заполнение данными из JSON
INSERT INTO boarding_passes (ticket_no, flight_id, boarding_no, seat_no, row, seat)
SELECT
    ticket_no,
    flight_id,
    boarding_no,
    seat_no,
    get_row(seat_no),
    get_seat(seat_no)
FROM bookings.boarding_passes;





Функции для имени пассажира
1. Напишите ещё две функции:
get_first_name — должна принимать на вход строку, содержащую Имя и Фамилию (например, " Иван Иванов") и возвращать только Имя (например, "Иван")
get_last_name — должна принимать на вход строку, содержащую Имя и Фамилию и возвращать только Фамилию
2. Измените представление tickets:
Обновите представление, чтобы оно включало два новых поля first_name и last_name, рассчитанных с использованием ранее созданных функций. Сначала пропишите Имя, потом Фамилию.

-- Создание функций для извлечения имени и фамилии
CREATE OR REPLACE FUNCTION get_first_name (passenger_name text) 
RETURNS text AS $$
    SELECT split_part(passenger_name, ' ', 1);
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION get_last_name (passenger_name text) 
RETURNS text AS $$
    SELECT split_part(passenger_name, ' ', 2);
$$ LANGUAGE sql;

-- Обновление представления tickets с добавлением имени и фамилии
CREATE OR REPLACE VIEW tickets AS
SELECT
    ticket_no::bpchar(13)     AS ticket_no,
    book_ref::bpchar(6)       AS book_ref,
    passenger_id::varchar(20) AS passenger_id,
    passenger_name::text      AS passenger_name,
    contact_data::jsonb       AS contact_data,
    get_first_name(passenger_name) AS first_name,
    get_last_name(passenger_name)  AS last_name
FROM tickets_raw;





Использование процедур для загрузки данных
Напишите одну процедуру с именем load_bookings_json, которая последовательно:

заполняет bookings
заполняет tickets_raw
заполняет ticket_flights_raw
Возьмите запросы из предыдущих шагов, но переделайте их с использованием MERGE. В процедуре ограничьте количество изменяемых строк в каждой таблице 100 строчками (используйте limit 100).
Добавьте в целевые таблицы поле _etl_updated_dttm, которое заполняйте текущим временем вставки (используйте функцию now() ) 

-- Добавление колонки для отслеживания обновлений
ALTER TABLE vj_volov.bookings ADD COLUMN IF NOT EXISTS _etl_updated_dttm timestamptz;
ALTER TABLE vj_volov.tickets_raw ADD COLUMN IF NOT EXISTS _etl_updated_dttm timestamptz;
ALTER TABLE vj_volov.ticket_flights_raw ADD COLUMN IF NOT EXISTS _etl_updated_dttm timestamptz;

-- Создание процедуры для загрузки данных
CREATE OR REPLACE PROCEDURE vj_volov.load_bookings_json()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Заполняем bookings с использованием MERGE
    MERGE INTO vj_volov.bookings b
    USING (
        SELECT 
            book_ref,
            book_date,
            total_amount,
            now() as _etl_updated_dttm
        FROM bookings.bookings_json
        LIMIT 100
    ) src ON b.book_ref = src.book_ref
    WHEN MATCHED THEN
        UPDATE SET
            book_date = src.book_date,
            total_amount = src.total_amount,
            _etl_updated_dttm = src._etl_updated_dttm
    WHEN NOT MATCHED THEN
        INSERT (book_ref, book_date, total_amount, _etl_updated_dttm)
        VALUES (src.book_ref, src.book_date, src.total_amount, src._etl_updated_dttm);

    -- Заполняем tickets_raw с использованием MERGE
    MERGE INTO vj_volov.tickets_raw tr
    USING (
        SELECT
            (js_elem->>'ticket_no') AS ticket_no,
            bj.book_ref AS book_ref,
            (js_elem->>'passenger_id') AS passenger_id,
            (js_elem->>'passenger_name') AS passenger_name,
            js_elem->>'contact_data' AS contact_data,
            now() as _etl_updated_dttm
        FROM bookings.bookings_json bj,
             LATERAL jsonb_array_elements(bj.json_data::jsonb) AS js_elem
        LIMIT 100
    ) src ON tr.ticket_no = src.ticket_no
    WHEN MATCHED THEN
        UPDATE SET
            book_ref = src.book_ref,
            passenger_id = src.passenger_id,
            passenger_name = src.passenger_name,
            contact_data = src.contact_data,
            _etl_updated_dttm = src._etl_updated_dttm
    WHEN NOT MATCHED THEN
        INSERT (ticket_no, book_ref, passenger_id, passenger_name, contact_data, _etl_updated_dttm)
        VALUES (src.ticket_no, src.book_ref, src.passenger_id, src.passenger_name, src.contact_data, src._etl_updated_dttm);

    -- Заполняем ticket_flights_raw с использованием MERGE
    MERGE INTO vj_volov.ticket_flights_raw tfr
    USING (
        SELECT
            (js_elem->>'ticket_no')       AS ticket_no,
            (flight->>'flight_id')        AS flight_id,
            (flight->>'fare_conditions')  AS fare_conditions,
            (flight->>'amount')           AS amount,
            now()                         AS _etl_updated_dttm
        FROM bookings.bookings_json bj,
             LATERAL jsonb_array_elements(bj.json_data::jsonb) AS js_elem,
             LATERAL jsonb_array_elements(js_elem->'flights') AS flight
        LIMIT 100
    ) src ON tfr.ticket_no = src.ticket_no AND tfr.flight_id = src.flight_id
    WHEN MATCHED THEN
        UPDATE SET
            fare_conditions = src.fare_conditions,
            amount = src.amount,
            _etl_updated_dttm = src._etl_updated_dttm
    WHEN NOT MATCHED THEN
        INSERT (ticket_no, flight_id, fare_conditions, amount, _etl_updated_dttm)
        VALUES (src.ticket_no, src.flight_id, src.fare_conditions, src.amount, src._etl_updated_dttm);

END;
$$;

-- Вызов процедуры
CALL vj_volov.load_bookings_json();





Создание витрины
Вам поручили сделать супер витрину!
Составьте запрос, который собирает информацию о рейсах, тарифных условиях, пассажирах, бронированиях и, если доступно, посадочных талонах для всех рейсов, которые:
запланированы не ранее 1 января 2016 года,
имеют статус "Arrived".
В запросе используйте view flight_airport_info и таблицы ticket_flights, tickets, bookings, boarding_passes. Добавьте все поля из перечисленных таблиц.
Результат положите в виде таблицы с названием fct_flights в свою схему.

-- Создание таблицы фактов для аналитики
DROP TABLE IF EXISTS vj_volov.fct_flights CASCADE;
CREATE TABLE vj_volov.fct_flights AS
SELECT 
    b.book_ref,
    b.book_date,
    b.total_amount,
    f.flight_no,
    f.flight_id,
    f.scheduled_departure,
    f.scheduled_arrival,
    f.aircraft_code,
    f.actual_departure,
    f.actual_arrival,
    f.status,
    tf.fare_conditions,
    tf.amount,
    t.ticket_no,
    t.passenger_id,
    t.passenger_name,
    t.contact_data,
    bp.boarding_no,
    bp.seat_no,
    bp.row,
    bp.seat
FROM vj_volov.bookings b
JOIN vj_volov.tickets t ON b.book_ref = t.book_ref
JOIN vj_volov.ticket_flights tf ON t.ticket_no = tf.ticket_no
JOIN vj_volov.flight_airport_info f ON tf.flight_id = f.flight_id
LEFT JOIN vj_volov.boarding_passes bp ON t.ticket_no = bp.ticket_no AND f.flight_id = bp.flight_id
WHERE f.scheduled_departure >= '2016-01-01'
AND f.status = 'Arrived';

