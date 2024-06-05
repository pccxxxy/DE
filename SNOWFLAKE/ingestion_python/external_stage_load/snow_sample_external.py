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


def upload_and_ingest(user, password, account, table, truncate):
    
    ctx = snowflake.connector.connect(user=user, password=password, account=account)
    # print(truncate)
    # exit()
    global cs
    cs = ctx.cursor()


    try:
        cs.execute("USE DATABASE DE_DEMO")
        cs.execute("USE SCHEMA PUBLIC")

        print("run SQLs")
        executeSqlFromFile("./snow_sample.sql")

        if truncate == "YES":
            print("Now truncating table {table}".format(table=table))
            cs.execute("truncate {table}".format(table=table))
            print("Table truncated")
            print("")

        print("run SQLs")
        res = cs.execute(
            """
                        copy into {table}
                        from @my_S3_stage/folder1
                        file_format = (format_name = CRIME_DEMO)
                 
                        """.format(
                table=table
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
        truncate = os.getenv("TRUNCATE", None)
    except:
        print("env variable read fail")
        sys.exit(2)

    upload_and_ingest(user, password, account, table, truncate)


if __name__ == "__main__":
    main()
