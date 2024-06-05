#!/usr/bin/env python
import snowflake.connector 
import sys, getopt
import os


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

# def upload_and_ingest(user, password, account, table, file, truncate, fn):
def upload_and_ingest(user, password, account, table, file, truncate):
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
        # executeSqlFromFile({fn})
        if truncate == "YES":
            print("Now truncating table {table}".format(table=table))
            cs.execute("truncate {table}".format(table=table))
            print("Table truncated")
            print("")
        print("run SQLs")
        print("Staging file {file} , please wait ...".format(file=file))
        res = cs.execute(
            r"put file://{file} @MY_CSV_STAGE OVERWRITE=TRUE auto_compress=true;".format(
                file=file
            )
        )
        # one_row = cs.fetchone()
        res = res.fetchall()[0]
        print(res)
        filename = os.path.basename(file)
        if "UPLOADED" in res:
            print("Successfully staged file {file}".format(file=file))
            print("")
            print("Loading file to table {table}, please wait...".format(table=table))
            res = cs.execute(
                """
                            copy into {table}
                            from @my_csv_stage
                            file_format = (format_name = CRIME_DEMO)
                            files = ('{filename}.gz')
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
    try:
        opts, args = getopt.getopt(sys.argv[1:], "u:p:a:t:f:d:")
        # opts, args = getopt.getopt(sys.argv[1:], "u:p:a:t:f:fn:d:")
    except getopt.GetoptError as err:
        print(err)
        print(
            "main.py -u <user> -p <password> -a <account> -t <table_name> -f <file_path>"
        )
        # print(
        #     "main.py -u <user> -p <password> -a <account> -t <table_name> -f <file_path> -fn <file_name>"
        # )
        sys.exit(2)
    for opt, arg in opts:
        if opt == "-h":
            print(
                "main.py -u <user> -p <password> -a <account> -t <table_name> -f <file_path>"
            )
            sys.exit()
        elif opt in ("-u"):
            user = arg
        elif opt in ("-p"):
            password = arg
        elif opt in ("-a"):
            account = arg
        elif opt in ("-t"):
            table = arg
        elif opt in ("-f"):
            file = arg
        elif opt in ("-d"):
            truncate = arg
    upload_and_ingest(user, password, account, table, file, truncate)


if __name__ == "__main__":
    main()
    
# Run this file by entering the following on zsh terminal
# path_to_python_interpreter snow_sample.py -u snowflake_username -p snowflake_password -a snowflake_accountname -t table_in_snowflake -f /path/to/rawdata.csv -d truncate_option_YES_or_NO