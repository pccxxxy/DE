# EXAMPLE BASH SCRIPT FOR ARRANGING THE ETL JOB CONSTRUCTED BY DIFFERENT COMPONENTS

#A. Create the bash script file 
# touch Temperature_ETL.sh   in terminal 


#B. Prepare the rest of the working files


#C. Write the bash script 

#! /bin/bash

# 1. Call the get_temp_api to extract reading 
# 2. Append reading to temperature.log
get_temp_api >> temperature.log

# 3. Overwrite the last 60 lines of the file
tail -60 temperature.log > temperature.log 

# 4. Call get_stats.py to 
# get the data from temperature.log, 
# then aggregate the readings, 
# and finally writes the aggregated result to temp_stats.csv
python3 get_stats.py temperature.log temp_stats.csv

# 5. Load the aggregated resut into the reporting system by 
# calling the reporting system API by incorporating the result csv as an argument
loads_stats_api temp_stats.csv


#D. Set the permission to make your shell script executable
# chmod u+x Temperature_ETL.sh   in terminal
# ls - l   in terminal - view the file permissions of the file in your current directory. 


#E. Schedule your job
# in terminal
# crontab -e   
# i   - to start inserting new content into crontab terminal editior
# 1**** <path>/Temperature_ETL.sh   - Fill the content in crontab terminal editor: run the Temperature_ETL.sh every minute daily
# press 'esc'  type :  type wq  hit return   - Save the content and exit






