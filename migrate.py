# Project Description: Migrate data from Google Sheets to Snowflake
# Author: Monty Dimkpa
# Version: 1.8
# Last Updated: May 2021
# Inquiries: cmdimkpa@gmail.com

# import required libraries
import os
import json
import sys
import snowflake.connector
import gspread
from time import sleep
import argparse

# determine script location and adjust for Operating System
THIS_DIR = os.getcwd()  
if "\\" in THIS_DIR:
    slash = "\\"
else:
    slash = "/"
THIS_DIR+=slash

# your local snowflake credentials file
snowflake_creds_file = THIS_DIR+"snowflake.json"

# your local Google Sheets API credentials file
googlesheets_creds_file = THIS_DIR+"googlesheets.json"

# extract required runtime arguments from CLI
parser = argparse.ArgumentParser()

def parse_csv(text, description):
    try:
        return [x for x in text.split(",") if x]
    except:
        print('Error: please provide [%s] as a comma-separated sequence of values e.g. 1,2,3,4' % description)
        sys.exit()

parser.add_argument('--rate-limit-delay', type=int, default=2.7, help='Seconds to wait before reading next row from Google Sheets API')
parser.add_argument('--max-concurrent-write', type=int, default=10, help='Number of rows to write to Snowflake at once')
parser.add_argument('--skip-first-row', type=bool, default=False, help='Option to skip the first row in your source data sheet')
parser.add_argument('--target-sheet-name', type=str, required=True, help='GOOGLESHEETS_TARGET_SHEET')
parser.add_argument('--columns-to-read', type=str, required=True, help='GOOGLESHEETS_READ_COLS (comma separated)')
parser.add_argument('--max-rows-to-copy', type=int, required=True, help='GOOGLESHEETS_MAX_ROW')
parser.add_argument('--target-warehouse', type=str, required=True, help='SNOWFLAKE_TARGET_WAREHOUSE')
parser.add_argument('--target-database', type=str, required=True, help='SNOWFLAKE_TARGET_DATABASE')
parser.add_argument('--target-table', type=str, required=True, help='SNOWFLAKE_TARGET_TABLE')
parser.add_argument('--target-schema', type=str, required=True, help='SNOWFLAKE_TARGET_SCHEMA')
parser.add_argument('--target-field-names', type=str, required=True, help='SNOWFLAKE_TARGET_FIELD_NAMES (comma separated)')
parser.add_argument('--target-role', type=str, required=True, help='SNOWFLAKE_TARGET_ROLE')

args = parser.parse_args()

# process arguments
RATE_LIMIT_DELAY_SECS = args.rate_limit_delay
CONCURRENT_WRITE_LIMIT_MAX = args.max_concurrent_write

# Google Sheets arguments
googlesheets_target_sheet = args.target_sheet_name
googlesheets_read_cols = parse_csv(args.columns_to_read, 'GOOGLESHEETS_READ_COLS')
googlesheets_max_row = args.max_rows_to_copy

# Snowflake arguments
snowflake_target_warehouse = args.target_warehouse
snowflake_target_database = args.target_database
snowflake_target_table = args.target_table
snowflake_target_schema = args.target_schema
snowflake_target_field_names = parse_csv(args.target_field_names, 'SNOWFLAKE_TARGET_FIELD_NAMES')
snowflake_target_role = args.target_role

# utility functions

def read_json_file(file):
    process = open(file,"rb+")
    creds_str = process.read()
    process.close()
    return json.loads(creds_str)

def apply_type(x):
    try:
        test = float(x)
        return x
    except:
        return "'%s'" % x

def convert(t):
    new_t = []
    for entry in t:
        comps = entry.split(",")
        if comps != ["'None'"]*len(comps):
            new_t.append("(%s)" % entry)
    return len(new_t), ",".join(new_t)

# pull your Snowflake credentials; create connection and cursor object
credsSF = read_json_file(snowflake_creds_file)
ctx = snowflake.connector.connect(
    user=credsSF["user"],
    password=credsSF["password"],
    account=credsSF["account"],
    warehouse=snowflake_target_warehouse,
    database=snowflake_target_database,
    schema=snowflake_target_schema
)
cs = ctx.cursor()

# connect to your Google Sheet
scope = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/drive'
]
client = gspread.service_account(filename='googlesheets.json')
GoogleSheet = client.open(googlesheets_target_sheet).sheet1

# your events log
eventlog = THIS_DIR+"events.log"

class EventManager:
    def __init__(self):
        try:
            self.eventlogProcess = open(eventlog, "rb+")
            self.realtimeLog = json.loads(self.eventlogProcess.read())
            self.eventlogProcess.close()
        except:
            self.realtimeLog = {
                "onRow": 0,
                "columnList": [],
                "fieldNames":[],
                "sql": []
            }
        self.sheetObject = None
        self.readLimit = None
        self.targetCursor = None
        self.targetTable = None
        self.eventlogProcess = None

    def update_target_table(self, table):
        self.targetTable = table
    
    def update_target_role(self, role):
        if self.targetCursor:
            self.targetCursor.execute("USE ROLE %s" % role)

    def update_sheet_object(self, sheet):
        self.sheetObject = sheet

    def update_target_cursor(self, cursor):
        self.targetCursor = cursor

    def update_read_cols(self, new_cols):
        self.realtimeLog["columnList"] = new_cols

    def update_field_names(self, names):
        self.realtimeLog["fieldNames"] = names

    def update_read_limit(self, max_row):
        self.readLimit = max_row

    def log(self):
        self.eventlogProcess = open(eventlog, "wb+")
        self.eventlogProcess.write(json.dumps(self.realtimeLog).encode())
        self.eventlogProcess.close()

    def run(self):
        if self.targetTable and self.sheetObject and self.readLimit and self.targetCursor and self.realtimeLog["columnList"] and self.realtimeLog["fieldNames"]:
            writes = 0
            query_tuple = ("%s.%s.%s" % (snowflake_target_database, snowflake_target_schema, self.targetTable), ",".join(self.realtimeLog["fieldNames"]),)
            values_tuple = ()
            while self.readLimit > self.realtimeLog["onRow"]:
                remaining_rows = self.readLimit - self.realtimeLog["onRow"]
                CONCURRENT_WRITE_LIMIT = CONCURRENT_WRITE_LIMIT_MAX if remaining_rows >= CONCURRENT_WRITE_LIMIT_MAX else remaining_rows
                self.SQLInsertTemplate = 'INSERT INTO %s (%s) VALUES ' + ','.join(['(%s)' for i in range(CONCURRENT_WRITE_LIMIT)])
                self.realtimeLog["onRow"] += 1
                try:
                    values = ",".join([apply_type(self.sheetObject.acell("%s%s" % (
                    column, self.realtimeLog["onRow"])).value) for column in self.realtimeLog["columnList"]])
                except:
                    print("Error: Google Rate Limit triggered. Try again after some time.")
                    self.shutdown()
                if not (writes == 0 and args.skip_first_row):
                    values_tuple += (values,)
                else:
                    args.skip_first_row = False
                if self.realtimeLog["onRow"] % CONCURRENT_WRITE_LIMIT == 0 or remaining_rows <= CONCURRENT_WRITE_LIMIT:
                    try:
                        use_tuple = query_tuple + values_tuple
                        sql = self.SQLInsertTemplate % use_tuple
                    except:
                        CONCURRENT_WRITE_LIMIT, values_tuple = convert(values_tuple)
                        sql = 'INSERT INTO %s (%s) VALUES '+values_tuple
                        sql = sql % ("%s.%s.%s" % (snowflake_target_database, snowflake_target_schema, self.targetTable), ",".join(self.realtimeLog["fieldNames"]))
                    try:
                        self.targetCursor.execute(sql)
                        self.realtimeLog["sql"].append(sql)
                        self.log()
                        writes+=CONCURRENT_WRITE_LIMIT
                        values_tuple = ()
                        print("200 OK on Write #%s - Query: %s" % (writes, sql))
                    except Exception as err:
                        err = str(err)
                        if 'EOF' not in err:
                            print("Error occured while writing: %s" % err)
                        self.shutdown()
                sleep(RATE_LIMIT_DELAY_SECS)
            self.shutdown()
        else:
            print("Error: please provide required command line arguments to begin")
            sys.exit()

    def shutdown(self):
        sys.exit()

# create and update migration event manager
currentEvents = EventManager()
currentEvents.update_sheet_object(GoogleSheet)
currentEvents.update_target_cursor(cs)
currentEvents.update_target_table(snowflake_target_table)
currentEvents.update_read_cols(googlesheets_read_cols)
currentEvents.update_field_names(snowflake_target_field_names)
currentEvents.update_read_limit(googlesheets_max_row)
currentEvents.update_target_role(snowflake_target_role)

# run event manager
currentEvents.run()

# session finished; close any open connections and exit
ctx.close()
sys.exit()
