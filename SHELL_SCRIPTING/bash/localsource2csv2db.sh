# 1. 
# Extracts data from /etc/passwd file into a CSV file.
# The csv data file contains the user name, user id and 
# home directory of each user account defined in /etc/passwd

# prompt users to start the extraction phase 
echo "Extracting data"
# extract the columns 1 (user name), 2 (user id) and 6 (home directory path) from /etc/passwd
cut -d":" -f1,3,6 /etc/passwd 
# redirect the extracted output into a file
cut -d":" -f1,3,6 /etc/passwd > extracted-data.txt



# 2.
# Transforms the text delimiter from ":" to ",".

# prompt users to start the transformation phase 
echo "Transforming data"
# read the extracted txt file and replace the colons with commas
tr ":" "," < extracted-data.txt
# read the extracted txt file, replace the colons with commas and save the changes to a csv
tr ":" "," < extracted-data.txt > transformed-data.csv


# 3.
# Loads the data from the CSV file into a table in PostgreSQL database.

# prompt users to start the loading phase 
echo "Loading data"
# send the instructions to connect to 'template1'
# copy the file to the table 'users' through command pipeline
echo "\c template1;\COPY users  FROM '/home/project/transformed-data.csv' DELIMITERS ',' CSV;" | psql --username=postgres --host=localhost