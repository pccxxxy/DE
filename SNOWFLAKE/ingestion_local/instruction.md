Purpose: 
Load Raw Data from your computer into Snowflake Cloud WH Locally




Tools used: 
- Snowflake account set up
- Data Warehouse and the database within data warehouse set up under your Snowflake acccount
- SnowSQL(Snowflake CLI) set up



Approach:

- Internal Staging

This is basically what we do. 
The raw data is stored as a file in a folder on our computer. 
We upload the file to the staging area on Snowflake. 

Note: 
External Staging can be adopted for other situations.
In that case, we have the file of raw data stored in a Cloud Provider's storaging service e.g. AWS S3. 
Then we upload the raw data from on-Cloud storage to the Snowflake staging area. 

- CLI 
Here we use CLI for its benefit of avoiding limitation.

Note: 
The same result can be achieved with the web interface of Snowflake. The web interface usually carries limitation at certain levels of operation though. 





Step-by-step guide for setting up SnowSQL:
1. install the SnowSQL

2(optional). Configure SnowSQL - Write your log-in details of the intended database in the configuration file of SnowSQL. These details are stored as variables in the configuration file. 

3. Make sure the SnowSQL is installed properly and is executable 

snowsql -v

4. connect to the intended database

snowsql -a $account_name -u $user_name -w COMPUTE_WH -d DEMO_DB -s PUBLIC



Step-by-step guide on data loading:

1. Check the input source (data profilling)

Key things we need to know about the input source
 - file format 
 - path
 - other characteristics of the input source that may affect the performance(bonus)

2. Create & Run target table SQL

create or replace TABLE CRIME_LITE (
INCIDENT_NUMBER VARCHAR(15) NOT NULL,
OFFENSE_CODE VARCHAR(10) NOT NULL,
OFFENSE_CODE_GROUP VARCHAR(255) NOT NULL,
OFFENSE_DESCRIPTION VARCHAR(255),
DISTRICT VARCHAR(10),
SHOOTING VARCHAR(2),
OCCURRED_ON_DATE TIMESTAMP_NTZ(9) NOT NULL,
YEAR NUMBER(38,0) NOT NULL,
MONTH NUMBER(38,0) NOT NULL,
DAY_OF_WEEK VARCHAR(20) NOT NULL,
HOUR NUMBER(38,0) NOT NULL,
UCR_PART VARCHAR(10),
STREET VARCHAR(255)
);

3. Connect to the intended database, using the CLI, SnowSQL. 

4. Create File Format

CREATE OR Replace FILE FORMAT "DE_DEMO"."PUBLIC".CRIME_DEMO
TYPE = 'CSV'
COMPRESSION = 'GZIP'
FIELD_DELIMITER = ','
RECORD_DELIMITER = '\n'
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '\042'
TRIM_SPACE = TRUE
ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
ESCAPE = 'NONE'
ESCAPE_UNENCLOSED_FIELD = '\134'
DATE_FORMAT = 'AUTO'
TIMESTAMP_FORMAT = 'dd/mm/yyyy HH24:MI'
NULL_IF = ('NULL');

5. Create Stage

CREATE or replace stage STAGE "DE_DEMO"."PUBLIC"."MY_CSV_STAGE1"
file_format = CRIME_DEMO; 

6. Put the file into Stage, specifying the storage location and file format
put file:///<path_to_file>/<sourcefilename_in_initialstorage> @my_csv_stage auto_compress=true; 


7. Start running data warehouse 

alter warehouse $mywarehouse resume;

8. Copy data into table 

copy into CRIME_LITE
from @my_csv_stage/crime2.csv.gz
file_format = (format_name = CRIME_DEMO)
on_error = 'skip_file';