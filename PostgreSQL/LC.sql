-- 175. Combine Two Tables

SELECT
    p1.firstName,
    p1.lastName,
    a1.city,
    a1.state
FROM Person AS p1
LEFT JOIN Address AS a1 ON p1.personId = a1.personId

--176. Second Highest Salary

SELECT (
    SELECT
        distinct(salary)
    FROM 
        Employee as e1
    ORDER BY e1.salary DESC
    LIMIT 1
    OFFSET 1
    ) as SecondHighestSalary

--178. Rank Scores

SELECT
    score,
    dense_rank() OVER (ORDER BY score desc) as rank 
FROM
    Scores

--180. Consecutive Numbers

WITH t1 AS (
  SELECT
    num,
    LAG(num) OVER (ORDER BY id) AS prev_num,
    LEAD(num) OVER (ORDER BY id) AS next_num
  FROM Logs
)
SELECT distinct(num) AS ConsecutiveNums
FROM t1
WHERE num = prev_num AND num = next_num

--181. Employees Earning More Than Their Managers

SELECT
    e1.name as Employee 
FROM 
    Employee as e1
JOIN  Employee as e2 ON e1.managerId = e2.id
WHERE
    e1.salary > e2.salary and ae1.managerid is not null

-- 182. Duplicate Emails

SELECT email
  FROM Person
 GROUP BY email
HAVING COUNT(1) > 1;

--183. Customers Who Never Order

SELECT 
    name as Customers
FROM
    Customers as c1
LEFT JOIN
    Orders as o2 ON c1.id = o2.customerId
WHERE 
    o2.id is null

--184. Department Highest Salary

WITH Rank AS (
    SELECT
        d2.name AS Department,
        e1.name AS Employee,
        e1.salary AS Salary,
        RANK() OVER (PARTITION BY d2.name ORDER BY e1.salary DESC) AS Rank
    FROM
        Employee AS e1
    JOIN 
        Department AS d2 ON e1.departmentId = d2.id
)
SELECT Department, Employee, Salary
FROM Rank
WHERE Rank = 1;

Найдите имена пилотов, которые в августе текущего года трижды летали в аэропорт "Шереметьево" в качестве второго пилота.

SELECT p1.name
FROM pilots p1
join flights f1 on p1.pilot_id = f1.second_pilot_id
where f1.destination = 'Шереметьево' AND EXTRACT(MONTH FROM f1.flight_dt) = 08
GROUP BY p1.name
HAVING COUNT(*) = 3

Выведите имена всех пилотов старше 45 лет, которые выполняли полёты на пассажирских самолётах с вместимостью более 30 человек.

select name
from pilots p1
join flights f1 on f1.first_pilot_id = p1.pilot_id or f1.second_pilot_id = p1.pilot_id
join planes p2 on p2.plane_id = f1.plane_id
where p2.cargo_flg = 0 and p1.age > 45 and p2.quantity > 30
GROUP BY name

Выведите 10 капитанов (first_pilot_id), совершивших наибольшее количество грузовых рейсов в 2022 году.
Отсортируйте результат по убыванию числа полётов.

SELECT pil.name
from pilots as pil
join flights as fli on pil.pilot_id = fli.first_pilot_id
join planes as plan on fli.plane_id = plan.plane_id
where plan.cargo_flg = 1 AND EXTRACT(YEAR FROM fli.flight_dt) = 2022
GROUP BY pil.pilot_id, pil.name
ORDER BY COUNT(*) DESC
limit 10

Необходимо написать SQL-запрос, который возвращает всех пользователей, опубликовавших более 10 постов.

select
    p1.id,
    p1.nickname
from
    profile p1
join post p2 on p2.owner_id = p1.id
GROUP By p1.id, p1.nickname
HAVING count(*) > 10

Найти пользователей, которые совершили заказы на товары из категории 'Electronics', с указанием общей суммы заказов по этой категории.

SELECT u1.id as user_id
    ,u1.name as user_name
    ,sum (o1.quantity * p1.price) as total_order_value
FROM users u1
join orders o1 on o1.user_id = u1.id
join products p1 on p1.id = o1.product_id
join categories c1 on p1.category_id = c1.id
WHERE c1.name = 'Electronics'
GROUP BY u1.id ,u1.name

У вас есть база данных, которая отслеживает проекты и сотрудников, назначенных на эти проекты. Вам необходимо проанализировать состав команд проектов, чтобы оценить средний уровень опыта.
Поля в результирующей таблице: project_id, average_years.
Округлите значения average_years до 2 знаков после запятой.

SELECT 
p1.project_id, 
round (avg(experience_years),2) as average_years
FROM
employee e1
join project p1 on e1.employee_id = p1.employee_id
GROUP BY p1.project_id

Выведите уникальные комбинации пользователя и id товара для всех покупок, совершенных пользователями до того, как их забанили. Результат отсортируйте в порядке возрастания сначала по имени пользователя, потом по SKU.
Если пользователь не был забанен, учитываются все его покупки.
Поля в результирующей таблице: user_id, first_name, last_name, sku.

SELECT 
    u1.id as user_id,
    u1.first_name,
    u1.last_name,
    p1.sku
FROM users u1
JOIN purchases p1 ON u1.id = p1.user_id
LEFT JOIN ban_list b1 ON u1.id = b1.user_id
WHERE b1.date_from IS NULL OR p1.date < b1.date_from
ORDER BY 2,4

Вывести в порядке убывания популярности доменные имена, используемые пользователями для электронной почты. 
Полученный результат необходимо дополнительно отсортировать по возрастанию названий доменных имён.

SELECT
    SPLIT_PART(email, '@', 2) as domain,
    count(1) as count
from users
GROUP BY SPLIT_PART(email, '@', 2)
order by 2 desc, 1 asc

185 hard
Руководители компании заинтересованы в том, чтобы узнать, кто зарабатывает больше всех в каждом отделе. 
Высокооплачиваемый сотрудник отдела — это сотрудник, чья зарплата входит в тройку самых высоких зарплат в этом отделе.
Напишите решение по поиску сотрудников с высокими доходами в каждом из отделов.
Верните таблицу результатов в любом порядке .
Формат результата показан в следующем примере.

with cte as (select
    d.name as Department
    ,e.name as Employee
    ,e.salary as Salary
    ,DENSE_RANK() over (partition by d.name order by e.salary desc) as rank
from Employee as e
join Department as d on d.id = e.departmentId
)
select 
    Department
    ,Employee
    ,Salary
from cte
where cte.rank <=3
ORDER BY Employee asc

550 Med 
Напишите решение, округляющее долю игроков, вошедших в игру повторно на следующий день после первого входа, до двух знаков после запятой. 
Другими словами, вам нужно определить количество игроков, вошедших в игру на следующий день после первого входа, и разделить это число на общее количество игроков.

SELECT 
    ROUND(CAST(SUM(CASE WHEN event_date = first_date + 1 THEN 1 ELSE 0 END) AS NUMERIC)
        /COUNT(DISTINCT player_id),2) AS fraction
FROM (SELECT 
        player_id,
        event_date,
        MIN(event_date) OVER (PARTITION BY player_id) AS first_date
    FROM Activity)

511 ez
Напишите решение для нахождения даты первого входа каждого игрока.
Верните таблицу результатов в любом порядке .

SELECT DISTINCT
    player_id
    , min(event_date) OVER (PARTITION BY player_id) AS first_login
FROM
    Activity
=======
262 hard
Коэффициент отмен рассчитывается путем деления количества отмененных (клиентом или водителем) запросов с незаблокированными пользователями 
на общее количество запросов с незаблокированными пользователями в этот день.
Напишите решение для поиска частоты отмен запросов с незабаненными пользователями ( и клиент, и водитель не должны быть забанены ) 
в день между "2013-10-01"и "2013-10-03"при наличии хотя бы одной поездки. Округлите Cancellation Rateдо двух знаков после запятой.
Верните таблицу результатов в любом порядке .

WITH cte AS (
    SELECT 
        request_at,
        COUNT(*) AS total,
        COUNT(CASE WHEN status = 'completed' THEN 1 END) AS completed
    FROM Trips AS t
    WHERE client_id NOT IN (SELECT users_id FROM Users WHERE banned = 'Yes')
      AND driver_id NOT IN (SELECT users_id FROM Users WHERE banned = 'Yes')
      AND request_at BETWEEN '2013-10-01' AND '2013-10-03'
    GROUP BY request_at
)
SELECT 
    request_at AS Day,
    ROUND(CASE WHEN total = 0 THEN 0.0
        ELSE CAST((total - completed) AS NUMERIC) / total
        END,2) AS "Cancellation Rate"
FROM cte
ORDER BY request_at ASC;

197. Rising Temperature
Напишите решение для поиска всех дат idс более высокими температурами по сравнению с предыдущими датами (вчера).

SELECT w1.id
FROM Weather w1
JOIN Weather w2
  ON w1.recordDate = w2.recordDate + INTERVAL '1 day'
WHERE w1.temperature > w2.temperature;

511.
Напишите решение для нахождения даты первого входа каждого игрока.
Верните таблицу результатов в любом порядке .

SELECT DISTINCT
    player_id
    , min(event_date) OVER (PARTITION BY player_id) AS first_login
FROM
    Activity

570 Med
Напишите решение, чтобы найти менеджеров с не менее чем пятью прямыми отчетами.

WITH CTE AS (SELECT e.id, e.name, e.department, e.managerId,
       COUNT(r.id) AS numReports
FROM Employee e
LEFT JOIN Employee r ON e.id = r.managerId
GROUP BY e.id, e.name, e.department, e.managerId
)

SELECT name
FROM CTE
WHERE numReports >=5