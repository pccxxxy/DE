#!/bin/bash
wget "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Bash%20Scripting/ETL%20using%20shell%20scripting/web-server-access-log.txt.gz"
gunzip -f web-server-access-log.txt.gz



echo "Extracting phase starts-----"

cut -d"#" -f1,2,3,4 /home/project/web-server-access-log.txt > transformed-data.csv

echo "Loading phase starts-----"
echo "\c template1;\COPY access_log(timestamp, latitude, longitude, visitorid) FROM '/home/project/transformed-data.csv' DELIMITER '#' CSV HEADER;" | psql --username=postgres --host=localhost



echo "Display the newly inserted records-----"

echo '\c template1; \\SELECT * from access_log;' | psql --username=postgres --host=localhost