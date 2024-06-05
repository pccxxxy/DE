-- SQL Worksheet for "Getting Started with Time Travel"

--** 1. Generate Database Objects **

-- create database
create or replace database timeTravel_db;
USE DATABASE timeTravel_db;

-- create new table in store and populate it with some data. You can always create a new table and load new data using the COPY command, but to simplify the process, let us use a table from the Snowflake sample database. To create a table from the sample database, use the following SQL.

CREATE TABLE STORE
AS SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.STORE;

-- Let us validate that our table has some data by running a count query on the table as follows.

SELECT COUNT(*) FROM STORE;
SELECT * FROM STORE LIMIT 100;

--** 2. Modify data before trying time_travel funcion

--Because our next step will be to perform an accidental update of the table, let us note the current time before we perform the next step. To do so, run the following SQL and make sure you save the output somewhere, e.g., in notepad.

SELECT CURRENT_TIMESTAMP;
-- 2024-05-03 22:48:28.176 -0700

-- Now let us update one of the table’s columns. The idea is to simulate an accidental update of a complete column.
UPDATE STORE SET s_manager = NULL;
SELECT * FROM STORE LIMIT 100;


--** 3. Query Your Time Travel Data **

-- get the database state at a specific date and time
SELECT * FROM STORE
AT(TIMESTAMP => '<timestamp>'::timestamp_ltz);

-- get the database state before a transaction occurred
SELECT * FROM STORE
BEFORE(TIMESTAMP => '<timestamp>'::timestamp_ltz);

-- get the database state from an offset of the current time
select * from STORE AT(offset => -60*5);


--** 4. Clone Past Database States **

-- clone table
create table STORE_2 clone STORE
  at(offset => -60*1);

  select * from STORE_2 limit 100;

--** 5. Cleanup and Know Your Options **

-- clean up
drop table if exists store;
drop table if exists store_2;
drop database if exists timeTravel_db;