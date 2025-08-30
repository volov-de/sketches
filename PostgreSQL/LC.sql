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