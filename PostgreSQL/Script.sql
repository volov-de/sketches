/*
Создание таблицы

Как было сказано в описании схемы: для города не предусмотрено отдельной сущности. Но мы считаем, что она будет полезна для нас и наших коллег-аналитиков, поэтому выделим её.

Создайте и заполните данными таблицу cities со следующими полями:

city_id (тип serial, не может быть NULL) — уникальный идентификатор города
city_name (тип text, не может быть NULL) — название города
latitude (тип double precision, не может быть NULL) — широта города
longitude (тип double precision, не может быть NULL) — долгота города
timezone (тип text, может быть NULL) — временная зона, в которой находится город
 Добавьте ограничения на поля широты и долготы (если вам нужно задать название для ограничений, используйте cities_latitude_check и cities_longitude_check).
*/

create table cities (
    city_id      SERIAL PRIMARY KEY NOT NULL,
    city_name    text NOT NULL,
    latitude     double precision NOT NULL,
    longitude    double precision NOT NULL,
    timezone     text NOT NULL,
    CONSTRAINT cities_latitude_check  CHECK (latitude BETWEEN -90 AND 90),
    CONSTRAINT cities_longitude_check CHECK (longitude BETWEEN -180 AND 180)
);


INSERT INTO cities (city_name, latitude, longitude, timezone)
select
a.city as city_name,
a.latitude,
a.longitude,
timezone
FROM bookings.airports a;



/*
1. Добавьте два новых поля в таблицу cities:
airport_count (тип integer, может быть NULL) — количество аэропортов в данном городе
departure_count (тип integer, может быть NULL) — количество вылетов за все время
*/
ALTER TABLE cities
ADD COLUMN airport_count integer;

ALTER TABLE cities
ADD column departure_count integer;
    

/*
2. Измените тип данных для поля city_name:
Установите тип данных varchar(255) вместо text. 
Это позволит ограничить длину названия города и лучше отразит специфику хранения данных
*/
ALTER TABLE cities
ALTER COLUMN city_name
TYPE varchar(255);



/*
3. Добавьте ограничение на поле airport_count ( назовите ограничение airport_count_nonnegative):
Установите ограничение, чтобы количество аэропортов было неотрицательным
*/
ALTER TABLE cities
ADD CONSTRAINT airport_count_nonnegative CHECK (airport_count >= 0);



/*
Вам поручено создать представление, которое объединяет информацию об аэропортах вылета и прилета для всех рейсов. 
Это представление будет служить важным инструментом для анализа и мониторинга полетов, облегчающим доступ к ключевой информации о каждом рейсе.

Создайте и заполните данными view с названием flight_airport_info, в котором будет содержаться:

номер рейса, идентификатор рейса и запланированное время вылета и прибытия,
вся информация об аэропортах прилета (используйте префикс для имен полей arrival_)
вся информация об аэропортах вылета (используйте префикс для имен полей departure_)
код воздушного судна,актуальное время вылета и прибытия, статус рейса

Например, правильное использование префикса для города вылета – departure_city
*/
DROP view flight_airport_info

CREATE OR REPLACE VIEW flight_airport_info AS
SELECT 
	f.flight_id,
	f.flight_no,
	f.scheduled_departure, 
	f.scheduled_arrival,
	a.airport_code as arrival_airport_code,
	a.airport_name as arrival_airport_name,
	a.city as arrival_city,
	a.longitude as arrival_longitude,
	a.latitude as arrival_latitude,
	a.timezone as arrival_timezone,
	a.airport_code as departure_airport_code,
    a.airport_name as departure_airport_name,
	a.city as departure_city,
	a.longitude as departure_longitude,
	a.latitude as departure_latitude,
	a.timezone as departure_timezone,
	f.aircraft_code,
	f.actual_departure,
	f.actual_arrival,
	f.status
	
FROM bookings.flights f
join bookings.airports a on a.airport_code = f.departure_airport or a.airport_code = f.arrival_airport





