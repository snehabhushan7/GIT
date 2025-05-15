-- Databricks notebook source
samples.tpch.customer

-- COMMAND ----------

DROP TABLE IF EXISTS Emp2

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS EMP1(
    Emp_ID INT,
    Name STRING,
    Salary DOUBLE,
    Dept STRING,
    Manager_ID INT,
    Dept_ID INT
    )

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS Emp1(
    EmpID INT,
    First_Name STRING,
    Last_Name STRING,
    Gender STRING,
    Salary DOUBLE,
    Team STRING
)
USING DELTA
LOCATION 'dbfs:/user/hive/warehouse/';

-- COMMAND ----------

ALTER TABLE emp1 SET TBLPROPERTIES (
   'delta.columnMapping.mode' = 'name',
   'delta.minReaderVersion' = '2',
   'delta.minWriterVersion' = '5')

-- COMMAND ----------

ALTER TABLE emp1 DROP COLUMN Manager_ID;

-- COMMAND ----------

ALTER TABLE emp1 ADD COLUMN Salary DOUBLE;

-- COMMAND ----------

SELECT * FROM DELTA.`dbfs:/user/hive/warehouse/emp1`

-- COMMAND ----------

SELECT * From DELTA.`dbfs:/user/hive/warehouse`

-- COMMAND ----------

DESCRIBE EXTENDED emp1

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm("dbfs:/user/hive/warehouse/", recurse=True)

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user

-- COMMAND ----------

Describe DETAIL emp1

-- COMMAND ----------

DESCRIBE SCHEMA workspace.default

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS EMP2(
    Emp_ID INT,
    Name STRING,
    Last_Name STRING,
    Salary DOUBLE,
    Dept STRING,
    Dept_ID INT
)

-- COMMAND ----------

INSERT INTO EMP1 
VALUES 
(1, "Shubham", 90000.00, "IT", 2, 1),
(2, "Aman", 80000.00, "ED", 1, 2),
(3, "Naveen", 120000.00, "PR", 1, 3),
(4, "Aditya", 25000.00, "ED", 2, 2),
(5, "Nishant. Salchichas", 50000.00, "IT", 1, 1)


-- COMMAND ----------

INSERT INTO EMP1
VALUES 
(3, "Emily", 70000.00, "ED", 1, 2),
(4, "Aditya", 25000.00, "ED", 2, 2)

-- COMMAND ----------

SELECT Emp_ID, Name, count(*) AS count FROM emp1 GROUP BY EMP_ID, Name
HAVING count >= 1

-- COMMAND ----------

INSERT INTO EMP2
VALUES 
(3, "Emily", "Cooper", 70000.00, "ED", 2),
(4, "John", "Ceina", 40000.00, "PR" , 3),
(5, "Vinny", "Vangara", 150000.00, "IT", 1),
(6, "Tom", "Ford", 30000.00, "PR" , 3),
(7, "Emma", " Christopher ", 60000.00, "ED", 2)


-- COMMAND ----------

INSERT INTO EMP1
VALUES 
(5, "Vinny", 150000.00, "IT", 1, 1)


-- COMMAND ----------

Select * from emp1


-- COMMAND ----------

SELECT Manager_ID, Dept_ID, Count(*) FROM emp1 GROUP BY Manager_ID, Dept_ID HAVING Count(*) > 1

-- COMMAND ----------

Select * from Emp2

-- COMMAND ----------



-- COMMAND ----------

Describe DETAIL emp1

-- COMMAND ----------

Describe DETAIL emp2

-- COMMAND ----------

DESCRIBE EXTENDED samples.tpch.customer

-- COMMAND ----------

select * from DELTA.`dbfs:/databricks-datasets/tpch/delta-001/customer`

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/databricks-datasets/tpch/

-- COMMAND ----------

DESCRIBE EXTENDED emp3

-- COMMAND ----------

Select * from Emp1 
INNER JOIN emp2
On Emp1.Emp_ID = Emp2.Emp_ID 
where Emp1.Dept_ID = Emp2.Dept_ID


-- COMMAND ----------

Select * from Emp1
full outer join Emp2 
On Emp1.Emp_ID = Emp2.Emp_ID


-- COMMAND ----------

SELECT * FROM Emp1 CROSS JOIN Emp2;

-- COMMAND ----------

Select salary from emp2

-- COMMAND ----------

Select max(salary) from emp2 where salary < (Select max(salary) from emp2)

-- COMMAND ----------

Select max(Salary) From emp1 where Salary < (Select max(Salary) FROM emp1)

-- COMMAND ----------

SELECT e.name as emp_name, e.salary as emp_salary , m.name as manager_name, m.salary as manager_salary 
from emp1 e
JOIN emp1 m on e.Emp_ID = m.Manager_ID
WHERE e.salary > m.salary

-- COMMAND ----------

SELECT dept_id, count(*) as emp_count FROM emp1 GROUP BY Dept_ID HAVING emp_count >= 1

-- COMMAND ----------

SELECT * FROM Emp1
UNION SELECT * FROM Emp2


-- COMMAND ----------

SELECT * FROM Emp1 
INTERSECT SELECT * FROM Emp2

-- COMMAND ----------

-- MAGIC %fs rm 'dbfs:/user/hive/warehouse/emp1'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm("dbfs:/user/hive/warehouse/_delta_log/", recurse=True)

-- COMMAND ----------

-- MAGIC %fs ls 
-- MAGIC

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/
-- MAGIC

-- COMMAND ----------

select * from delta.`dbfs:/user/hive/warehouse/emp3`;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS EMP3 
AS select * from delta.`dbfs:/user/hive/warehouse/emp1`;

-- COMMAND ----------

VACUUM delta.`dbfs:/user/hive/warehouse/emp3`;

-- COMMAND ----------

OPTIMIZE delta.`dbfs:/user/hive/warehouse/emp3`
ZORDER BY (Salary);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS Emp1(
    EmpID INT,
    First_Name STRING,
    Last_Name STRING,
    Gender STRING,
    Salary DOUBLE,
    Team STRING
)
USING DELTA
LOCATION 'dbfs:/user/hive/warehouse/';



-- COMMAND ----------

DESCRIBE EXTENDED emp1

-- COMMAND ----------

ALTER TABLE emp1 DROP COLUMN Salary;