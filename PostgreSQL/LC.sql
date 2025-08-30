-- 175. Combine Two Tables

SELECT
    p1.firstName,
    p1.lastName,
    a1.city,
    a1.state
FROM Person AS p1
LEFT JOIN Address AS a1 ON p1.personId = a1.personId