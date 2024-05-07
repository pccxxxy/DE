-- Practice on the Snowflake initalised data 

SELECT s.*
FROM
    (
        SELECT 'Hi,Snow,Flake' AS TestString
    ) tmp
    , LATERAL FLATTEN (INPUT => SPLIT(tmp.TestString,',')) s;


SELECT s.value
FROM
    (
        SELECT 'Hi,Snow,Flake' AS TestString
    ) tmp
    , LATERAL FLATTEN (INPUT => SPLIT(tmp.TestString,',')) s;
    

select split('Hi,Snow,Flake', ',');

select split('Hi,Snow,Flake', ',')[1]::string;






-- STEP 1. SET UP DATABASE, DATA WAREHOUSE AND TABLES


create or replace database mydatabase;

-- Specify the active database and schema for the current user session. Specifying
-- a database now enables you to perform your work in this database without having
-- to provide the name each time it is requested.

use schema mydatabase.public;

-- Create a target table for the JSON data

create or replace table raw_source (
  src variant);

-- Create a warehouse

create or replace warehouse mywarehouse with
  warehouse_size='X-SMALL'
  auto_suspend = 120
  auto_resume = true
  initially_suspended=true;





-- STEP 2. IMPORT DATA INTO TABLE IN NOMINATED DATABASE WITHIN DATA WAREHOUSE, USING THE FEATURE OF SNOWFLAKE EXTERNAL IMPORT 


-- Specify the active warehouse for the current user session.
-- Snowflake does not actually require or use a warehouse until you load data using the COPY command.

use warehouse mywarehouse;

-- Create an external stage that points to the S3 bucket containing the sample file for this tutorial

create or replace stage my_stage  -- Import raw data which is a file stored in AWS S3 bucket
  url = 's3://snowflake-docs/tutorials/json';
  
alter warehouse mywarehouse resume;
  
copy into raw_source
from @my_stage/server/2.6/2016/07/15/15
file_format = (type = json);


-- RETRIEVE DATA
select * from raw_source;

select src:events[1]:f
from raw_source;


select src:device_type::string as device_type
from raw_source;

select
  src:events
from raw_source;
  
select
value:f::number
from
  raw_source
, lateral flatten( input => src:events );


select src:device_type::string
, src:version::string
, value
from
  raw_source
, lateral flatten( input => src:events );





-- STEP 3. CREATE ANOTHER TABLE, STORING THE TABLE DATA PREVIOUSLY IMPORTED FROM S3


create or replace table events as
  select
    src:device_type::string                             as device_type
  , src:version::string                                 as version
  , value:f::number                                     as f
  , value:rv::variant                                   as rv
  , value:t::number                                     as t
  , value:v.ACHZ::number                                as achz
  , value:v.ACV::number                                 as acv
  , value:v.DCA::number                                 as dca
  , value:v.DCV::number                                 as dcv
  , value:v.ENJR::number                                as enjr
  , value:v.ERRS::number                                as errs
  , value:v.MXEC::number                                as mxec
  , value:v.TMPI::number                                as tmpi
  , value:vd::number                                    as vd
  , value:z::number                                     as z
  from
    raw_source
  , lateral flatten ( input => SRC:events );