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





