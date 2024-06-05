#!/usr/bin/env python
import snowflake.connector
import sys, getopt
import os
from dotenv import load_dotenv


def executeSqlFromFile(filename):
    # Open and read the file as a single buffer
    fd = open(filename, "r")
    sqlFile = fd.read()
    fd.close()

    # all SQL commands (split on ';')
    sqlCommands = sqlFile.split(";")
    # Execute every command from the input file
    for command in sqlCommands:
        # This will skip and report errors
        # For example, if the tables do not yet exist, this will skip over
        # the DROP TABLE commands
        try:
            print(command)
            cs.execute(command)
        except:
            print("Command skipped")


def upload_and_ingest(user, password, account, table, sqlfile, rawdatafile, truncate):

    ctx = snowflake.connector.connect(user=user, password=password, account=account)
    # print(truncate)
    # exit()
    global cs
    cs = ctx.cursor()


    try:
        cs.execute("USE DATABASE DE_DEMO")
        cs.execute("USE SCHEMA PUBLIC")
        
        print("run SQLs")
        # executeSqlFromFile("./snow_sample.sql")
        print(sqlfile)
        print(type(sqlfile))
        print({sqlfile})
        print(type({sqlfile}))
        executeSqlFromFile("{placeholderfile}".format(placeholderfile=sqlfile))
        # Alternatively, simplify the above line if environment variable fn is initially passed in as a string 
        # executeSqlFromFile(fn)
        
        if truncate == "YES":
            print("Now truncating table {table}".format(table=table))
            cs.execute("truncate {table}".format(table=table))
            print("Table truncated")
            print("")
        print("run SQLs")
        print("Staging file {file} , please wait ...".format(file=rawdatafile))
        res = cs.execute(
            r"put file://{file} @MY_CSV_STAGE OVERWRITE=TRUE auto_compress=true;".format(
                file=rawdatafile
            )
        )
        # one_row = cs.fetchone()
        res = res.fetchall()[0]
        print(res)
        filename = os.path.basename(rawdatafile)
        if "UPLOADED" in res:
            print("Successfully staged file {file}".format(file=rawdatafile))
            print("")
            print("Loading file to table {table}, please wait...".format(table=table))
            res = cs.execute(
                """
                            copy into {table}
                            from @my_csv_stage
                            file_format = (format_name = CRIME_DEMO)
                            files = ('{filename}.gz')
                            on_error = 'skip_file';
                            """.format(
                    table=table, filename=filename
                )
            )
            res = res.fetchall()[0]
            print(res)
            if "LOADED" in res:
                print("Successfully loaded data into table {table}".format(table=table))
        else:
            pass
    finally:
        cs.close()
    ctx.close()


# main function to start the process
def main():
    load_dotenv()
    try:
        user = os.getenv("SF_USER", None)
        password = os.getenv("PASSWORD", None)
        account = os.getenv("ACCOUNT", None)
        table = os.getenv("TABLE", None)
        sqlfile = os.getenv("SQLFILE", None)
        rawdatafile = os.getenv("RAWDATAFILE", None)
        truncate = os.getenv("TRUNCATE", None)
        
    except:
        print("env variable read fail")
        sys.exit(2)

    # Check if the sql_file_path is None
    if sqlfile is None:
        raise ValueError("The SQL_FILE_PATH environment variable is not set")
    
    
    upload_and_ingest(user, password, account, table, sqlfile, rawdatafile, truncate)


if __name__ == "__main__":
    main()
