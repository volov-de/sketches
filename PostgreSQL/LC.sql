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